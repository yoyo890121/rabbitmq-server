%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_modern_queue_index).

-export([erase/1, init/3, reset_state/1, recover/6,
         terminate/3, delete_and_terminate/1,
         publish/6, deliver/2, ack/2, read/3]).

%% Recovery. Unlike other functions in this module, these
%% apply to all queues all at once.
-export([start/2, stop/1]).

%% rabbit_queue_index/rabbit_variable_queue-specific functions.
%% Implementation details from the queue index leaking into the
%% queue implementation itself.
-export([pre_publish/7, flush_pre_publish_cache/2,
         sync/1, needs_sync/1, flush/1,
         bounds/1, next_segment_boundary/1]).

-define(QUEUE_NAME_STUB_FILE, ".queue_name").
-define(SEGMENT_EXTENSION, ".midx").

-define(MAGIC, 16#524D5149). %% "RMQI"
-define(VERSION, 1).
-define(HEADER_SIZE, 64). %% bytes
-define(ENTRY_SIZE,  32). %% bytes

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("kernel/include/file.hrl").

-type seq_id() :: non_neg_integer().
%% @todo Use a shared seq_id() type in all relevant modules.

-record(mqistate, {
    %% Queue name (for the stub file).
    queue_name :: rabbit_amqqueue:name(),

    %% Queue index directory.
    dir :: file:filename(),

    %% seq_id() of the next message to be written
    %% in the index. Messages are written sequentially.
    %% Messages are always written to the index.
    write_marker = 0 :: seq_id(),

    %% seq_id() of the next message to be read from disk.
    %% After the read_marker position there may be messages
    %% that are in transit (in the in_transit list) that
    %% will be skipped when setting the next read_marker
    %% position, or acked (a flag on disk).
    %%
    %% When messages in in_transit are requeued we lower
    %% the read_marker position to the lowest value.
    read_marker = 0 :: seq_id(),

    %% seq_id() of messages that are in-transit. They
    %% could be in-transit because they have been read
    %% by the queue, or delivered to the consumer.
    in_transit = [] :: [seq_id()],

    %% seq_id() of the highest contiguous acked message.
    %% All messages before and including this seq_id()
    %% have been acked. We use this value both as an
    %% optimization and to know which segment files
    %% have been fully acked and can be deleted.
    %%
    %% In addition to the ack marker, messages in the
    %% index will also contain an ack byte flag that
    %% indicates whether that message was acked.
    ack_marker = undefined :: seq_id() | undefined,

    %% seq_id() of messages that have been acked
    %% and are higher than the ack_marker. Messages
    %% are only listed here when there are unacked
    %% messages before them.
    acks = [] :: [seq_id()],

    %% Oldest segment file number with un-acked messages.
    %% Roughly where we are currently reading messages from.
    oldest_segment = 0 :: non_neg_integer(),

    %% Newest segment file number with un-acked messages.
    %% Where we are writing newly published messages to.
    newest_segment = 0 :: non_neg_integer(),

    %% File descriptors.
    %% @todo The current implementation does not limit the number of FDs open.
    %%       This is most likely not desirable... Perhaps we can have a limit
    %%       per queue index instead of relying on file_handle_cache.
    write_fd = undefined :: file:fd() | undefined,
    read_write_fds = #{} :: #{non_neg_integer() => file:fd()},

    %% These funs are specific to the rabbit_variable_queue
    %% implementation. It expects us to call them when we
    %% have done a sync to the file system. Because we
    %% are never doing an explicit sync we cannot call
    %% them then so we instead call them as soon as we
    %% have written to the file (even if the data sits
    %% in the file cache and not on disk for a while).
    %%
    %% @todo Perhaps it could be useful to have an option
    %% that defines whether we do syncs to keep the same
    %% behavior as before for people who need it. But the
    %% default should be no manual syncs for performance
    %% reasons.
    on_sync :: on_sync_fun(),
    on_sync_msg :: on_sync_fun()
}).

-type mqistate() :: #mqistate{}.
-export_type([mqistate/0]).

%% Types copied from rabbit_queue_index.

-type on_sync_fun() :: fun ((gb_sets:set()) -> ok). %% @todo OK not sure what I should do about that at all.
-type contains_predicate() :: fun ((rabbit_types:msg_id()) -> boolean()).
-type shutdown_terms() :: list() | 'non_clean_shutdown'.

%% ----

-spec erase(rabbit_amqqueue:name()) -> 'ok'.

erase(#resource{ virtual_host = VHost } = Name) ->
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    Dir = queue_dir(VHostDir, Name),
    erase_index_dir(Dir).

-spec init(rabbit_amqqueue:name(),
                 on_sync_fun(), on_sync_fun()) -> mqistate().

init(#resource{ virtual_host = VHost } = Name, OnSyncFun, OnSyncMsgFun) ->
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    Dir = queue_dir(VHostDir, Name),
    false = rabbit_file:is_file(Dir), %% is_file == is file or dir
    init1(Name, Dir, OnSyncFun, OnSyncMsgFun).

init1(Name, Dir, OnSyncFun, OnSyncMsgFun) ->
    ensure_queue_name_stub_file(Name, Dir),
    open_write_file(new, #mqistate{
        queue_name = Name,
        dir = Dir,
        on_sync = OnSyncFun,
        on_sync_msg = OnSyncMsgFun
    }).

ensure_queue_name_stub_file(#resource{virtual_host = VHost, name = QName}, Dir) ->
    QueueNameFile = filename:join(Dir, ?QUEUE_NAME_STUB_FILE),
    ok = filelib:ensure_dir(QueueNameFile),
    ok = file:write_file(QueueNameFile, <<"VHOST: ", VHost/binary, "\n",
                                          "QUEUE: ", QName/binary, "\n">>).

open_write_file(Reason, State = #mqistate{ write_marker = WriteMarker }) ->
    SegmentEntryCount = segment_entry_count(),
    Segment = WriteMarker div SegmentEntryCount,
    %% We only use 'read' here so the file doesn't get truncated.
    {ok, Fd} = file:open(segment_file(Segment, State), [read, write, raw]),
    %% When we are recovering from a shutdown we may need to check
    %% whether the file already has content. If it does, we do
    %% nothing. Otherwise, as well as in the normal case, we
    %% pre-allocate the file size and write in the file header.
    IsNewFile = case Reason of
        new ->
            true;
        recover ->
            {ok, #file_info{ size = Size }} = file:read_file_info(Fd),
            Size =:= 0
    end,
    case IsNewFile of
        true ->
            %% We preallocate space for the file when possible.
            %% We don't worry about failure here because an error
            %% is returned when the system doesn't support this feature.
            %% The index will simply be less efficient in that case.
            _ = file:allocate(Fd, 0, ?HEADER_SIZE + SegmentEntryCount * ?ENTRY_SIZE),
            %% We then write the segment file header. It contains
            %% some useful info and some reserved bytes for future use.
            %% We currently do not make use of this information. It is
            %% only there for forward compatibility purposes (for example
            %% to support an index with mixed segment_entry_count() values).
            FromSeqId = (WriteMarker div SegmentEntryCount) * SegmentEntryCount,
            ToSeqId = FromSeqId + SegmentEntryCount,
            ok = file:write(Fd, << ?MAGIC:32,
                                   ?VERSION:8,
                                   FromSeqId:64/unsigned,
                                   ToSeqId:64/unsigned,
                                   0:344 >>);
        false ->
            ok
    end,
    %% All good.
    State#mqistate{ write_fd = Fd }.

-spec reset_state(State) -> State when State::mqistate().

reset_state(State = #mqistate{ queue_name     = Name,
                               dir            = Dir,
                               on_sync        = OnSyncFun,
                               on_sync_msg    = OnSyncMsgFun }) ->
    delete_and_terminate(State),
    init1(Name, Dir, OnSyncFun, OnSyncMsgFun).

-spec recover(rabbit_amqqueue:name(), shutdown_terms(), boolean(),
                    contains_predicate(),
                    on_sync_fun(), on_sync_fun()) ->
                        {'undefined' | non_neg_integer(),
                         'undefined' | non_neg_integer(), mqistate()}.

recover(#resource{ virtual_host = VHost } = Name, Terms, MsgStoreRecovered,
        ContainsCheckFun, OnSyncFun, OnSyncMsgFun) ->
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    Dir = queue_dir(VHostDir, Name),
    State0 = init1(Name, Dir, OnSyncFun, OnSyncMsgFun),
    %% We first recover the oldest/newest segment numbers, either
    %% from the terms or from the file names on disk.
    {Oldest, Newest} = case Terms =/= non_clean_shutdown of
        true ->
            %% @todo Both ack_marker and acks should also be stored in terms.
            %% @todo Maybe we can also keep a read_marker as a small optimization as well.
            %% In that case the read_marker would be min(read_marker, min(in_transit)).
            proplists:get_value(mqi_segments, Terms, {0, 0});
        false ->
            %% @todo We also need to figure out what the write_marker is!!
            %%       On the other hand the ack_marker can be set to 'undefined'
            %%       when (ReadMarker = 0) or (ReadMarker - 1) otherwise.
            recover_segments(Dir)
    end,
    %% We set the read marker to the first message in the oldest
    %% segment. The acked messages will be skipped automatically.
    %% @todo Do this.
    %% @todo Only do this when we have a dirty init.
    State1 = State0#mqistate{ oldest_segment = Oldest,
                              newest_segment = Newest },
    %% If the message store had to recover, we go through
    %% messages in the segments to make sure we are synced
    %% with the messages in the store.
    {Count, Bytes, State2} = case MsgStoreRecovered of
        true ->
            recover_sync_with_msg_store(ContainsCheckFun, State1);
        false ->
            {undefined, undefined, State1}
    end,
    %% We set the read marker to the first message in
    %% the oldest segment. The first read/2 call will then skip
    %% to the first un-acked message automatically. We do not
    %% need to worry about acked and unacked messages being
    %% interspersed because of this skip mechanism.
    State = State2#mqistate{ read_marker = Oldest * segment_entry_count() },
    %% We can now open the write file and return.
    {Count, Bytes, open_write_file(recover, State)}.

%% This function will not play nice if there are non-integers
%% file names with the same extension as segment files.

recover_segments(Dir) ->
    %% @todo We also need to look inside the files to recover ack_marker and acks.
    Nums = lists:sort([
        list_to_integer(filename:basename(F, ?SEGMENT_EXTENSION))
    || F <- rabbit_file:wildcard(".*\\" ++ ?SEGMENT_EXTENSION, Dir)]),
    case Nums of
        [] -> {0, 0};
        [N] -> {N, N};
        [Oldest|Tail] -> {Oldest, hd(lists:reverse(Tail))}
    end.

%% We mark messages as acked if they are no longer in the message
%% store. This does not necessarily mean that the message was
%% sent to a consumer.

recover_sync_with_msg_store(_ContainsCheckFun, _State) ->
    todo.

-spec terminate(rabbit_types:vhost(), [any()], State) -> State when State::mqistate().

terminate(VHost, Terms, State = #mqistate { dir = Dir,
                                            write_fd = WriteFd,
                                            read_write_fds = OpenFds,
                                            oldest_segment = Oldest,
                                            newest_segment = Newest }) ->
    %% Fsync and close all FDs.
    ok = file:sync(WriteFd),
    ok = file:close(WriteFd),
    _ = maps:map(fun(_, Fd) ->
        ok = file:sync(Fd),
        ok = file:close(Fd)
    end, undefined, OpenFds),
    %% Write recovery terms for faster recovery.
    rabbit_recovery_terms:store(VHost, filename:basename(Dir),
                                [{mqi_segments, {Oldest, Newest}} | Terms]),
    State#mqistate{ write_fd = undefined, read_write_fds = #{} }.

-spec delete_and_terminate(State) -> State when State::mqistate().

delete_and_terminate(State = #mqistate { dir = Dir,
                                         write_fd = WriteFd,
                                         read_write_fds = OpenFds }) ->
    %% Close all FDs.
    ok = file:close(WriteFd),
    _ = maps:map(fun(_, Fd) ->
        ok = file:close(Fd)
    end, undefined, OpenFds),
    %% Erase the data on disk.
    ok = erase_index_dir(Dir),
    State#mqistate{ write_fd = undefined, read_write_fds = #{} }.

-spec publish(rabbit_types:msg_id(), seq_id(),
                    rabbit_types:message_properties(), boolean(),
                    non_neg_integer(), State) -> State when State::mqistate().

publish(_, SeqId, _, _, _, State = #mqistate { write_marker = WriteMarker,
                                               read_marker = ReadMarker0,
                                               in_transit = InTransit })
        when SeqId < WriteMarker ->
    %% We have already written this message on disk. We do not need
    %% to write it again. We may instead remove it from the in_transit list.
    %% @todo Confirm that this is indeed the behavior we should have.
    %% @todo It would be better to have a separate function for this...
    ReadMarker = if
        SeqId < ReadMarker0 -> SeqId;
        true -> ReadMarker0
    end,
    State#mqistate{ read_marker = ReadMarker,
                    in_transit = InTransit -- [SeqId] };
publish(MsgOrId, SeqId, Props, IsPersistent, _TargetRamCount,
        State = #mqistate { write_marker = WriteMarker,
                            write_fd = WriteFd })
        when SeqId =:= WriteMarker ->
    %% Prepare the entry to be written.
    Id = case MsgOrId of
        #basic_message{id = Id0} -> Id0;
        Id0 when is_binary(Id0) -> Id0
    end,
    Flags = case IsPersistent of
        true -> 1;
        false -> 0
    end,
    #message_properties{ expiry = Expiry0, size = Size } = Props,
    Expiry = case Expiry0 of
        undefined -> 0;
        _ -> Expiry0
    end,
    Entry = << 0:8,                   %% ACK.
               0:8,                   %% Deliver.
               Flags:8,               %% IsPersistent flag (least significant bit).
               0:8,                   %% Reserved. Makes entries 32B in size to match page alignment on disk.
               Id:16/binary,          %% Message store ID.
               Size:32/unsigned,      %% Message payload size.
               Expiry:64/unsigned >>, %% Expiration time.
    ok = file:write(WriteFd, Entry),
    NextWriteMarker = WriteMarker + 1,
    %% When the write_marker ends up on a new segment we must
    %% sync, close this file and open the next. We sync here
    %% because we want to avoid having two partially written
    %% segment files in case of dirty shutdowns.
    SegmentEntryCount = segment_entry_count(),
    ThisSegment = WriteMarker div SegmentEntryCount,
    NextSegment = NextWriteMarker div SegmentEntryCount,
    case ThisSegment =:= NextSegment of
        true ->
            State#mqistate{ write_marker = NextWriteMarker };
        false ->
            ok = file:sync(WriteFd),
            ok = file:close(WriteFd),
            open_write_file(new, State#mqistate{ write_marker = NextWriteMarker,
                                                 write_fd = undefined })
    end.

%% When marking delivers we only need to update the file(s) on disk.

-spec deliver([seq_id()], State) -> State when State::mqistate().

%% The rabbit_variable_queue module may call this function
%% with an empty list. Do nothing.
deliver([], State) ->
    State;
deliver(SeqIds, State0) ->
    lists:foldl(fun(SeqId, State1) ->
        {Fd, OffsetForSeqId, State} = get_fd(SeqId, State1),
        {ok, _} = file:position(Fd, OffsetForSeqId + 1),
        ok = file:write(Fd, <<1>>),
        State
    end, State0, SeqIds).

%% When marking acks we need to update the file(s) on disk
%% as well as update ack_marker and/or acks in the state.
%% When a file has been fully acked we may also close its
%% open FD if any and delete it.

-spec ack([seq_id()], State) -> State when State::mqistate().

%% The rabbit_variable_queue module may call this function
%% with an empty list. Do nothing.
ack([], State) ->
    State;
ack(SeqIds, State0 = #mqistate{ ack_marker = AckMarkerBefore }) ->
    State = lists:foldl(fun(SeqId, State1) ->
        {Fd, OffsetForSeqId, State2} = get_fd(SeqId, State1),
        {ok, _} = file:position(Fd, OffsetForSeqId),
        ok = file:write(Fd, <<1>>),
        update_ack_state(SeqId, State2)
    end, State0, SeqIds),
    maybe_delete_segments(AckMarkerBefore, State).

update_ack_state(SeqId, State = #mqistate{ ack_marker = undefined, acks = Acks }) ->
    %% We must special case when there is no ack_marker in the state.
    %% The message with seq_id() 0 has not been acked before now.
    case SeqId of
        0 -> State#mqistate{ ack_marker = 0 };
        _ -> State#mqistate{ acks = [SeqId|Acks] }
    end;
update_ack_state(SeqId, State = #mqistate{ read_marker = ReadMarker0,
                                           ack_marker = AckMarker0,
                                           acks = Acks }) ->
    case AckMarker0 + 1 of
        %% When SeqId is the message after the marker, we update the marker
        %% and potentially remove additional SeqIds from the acks list. The
        %% new ack marker becomes either SeqId or the largest continuous
        %% seq_id() following SeqId found in Acks.
        SeqId ->
            AckMarker = highest_continuous_seq_id(lists:sort([SeqId|Acks])),
            RemainingAcks = lists:dropwhile(
                fun(AckSeqId) -> AckSeqId =< AckMarker end,
                lists:sort(Acks)),
            %% When the ack marker gets past the read marker, we need to
            %% advance the read marker.
            %% @todo This is only necessary if acks can arrive after a
            %%       dirty shutdown I believe.
            ReadMarker = if
                AckMarker >= ReadMarker0 ->
                    AckMarker + 1;
                true ->
                    ReadMarker0
            end,
            State#mqistate{ read_marker = ReadMarker,
                            ack_marker = AckMarker,
                            acks = RemainingAcks };
        %% Otherwise we simply add the SeqId to the acks list.
        _ ->
            State#mqistate{ acks = [SeqId|Acks] }
    end.

maybe_delete_segments(AckMarkerBefore, State = #mqistate{ ack_marker = AckMarkerAfter }) ->
    SegmentEntryCount = segment_entry_count(),
    SegmentBefore = case AckMarkerBefore of
        undefined -> 0;
        _ -> AckMarkerBefore div SegmentEntryCount
    end,
    SegmentAfter = AckMarkerAfter div SegmentEntryCount,
    if
        %% When the marker still points to the same segment,
        %% do nothing.
        SegmentBefore =:= SegmentAfter ->
            State;
        %% When the marker points to a different segment,
        %% delete fully acked segment files.
        true ->
            delete_segments(SegmentBefore, SegmentAfter - 1, State)
    end.

delete_segments(Segment, Segment, State) ->
    delete_segment(Segment, State);
delete_segments(Segment, LastSegment, State) ->
    delete_segments(Segment + 1, LastSegment,
        delete_segment(Segment, State)).

%% We do not need to worry about closing the write_fd because it is
%% not possible to have a fully acked segment file also opened as
%% a write file. The writer closes the file as soon as it reaches
%% the maximum number of entries.

delete_segment(Segment, State0 = #mqistate{ read_write_fds = OpenFds0 }) ->
    %% We close the open fd if any.
    State = case maps:take(Segment, OpenFds0) of
        {Fd, OpenFds} ->
            ok = file:close(Fd),
            State0#mqistate{ read_write_fds = OpenFds };
        error ->
            State0
    end,
    %% Then we can delete the segment file.
    ok = file:delete(segment_file(Segment, State)),
    State.

%% A better interface for read/3 would be to request a maximum
%% of N messages, rather than first call next_segment_boundary/3
%% and then read from S1 to S2. This function could then return
%% either N messages or less depending on the current state.

-spec read(seq_id(), seq_id(), State) ->
                     {[{rabbit_types:msg_id(), seq_id(),
                        rabbit_types:message_properties(),
                        boolean(), boolean()}], State}
                     when State::mqistate().

%% From is inclusive, To is exclusive.
%% @todo This function is incompatible with the rabbit_queue_index function.
%%       In our case we may send messages >= ToSeqId. As a result the
%%       rabbit_variable_queue module will need to be accomodated.

read(FromSeqId, FromSeqId, State) ->
    {[], State};
read(FromSeqId, ToSeqId, State) ->
    read(ToSeqId - FromSeqId, State).

%% There might be messages after the read marker that were acked.
%% We need to skip those messages when we attempt to read them.

read(_, State = #mqistate{ write_marker = WriteMarker, read_marker = ReadMarker })
        when WriteMarker =:= ReadMarker ->
    {[], State};
read(Num, State0 = #mqistate{ write_marker = WriteMarker, read_marker = ReadMarker,
                              in_transit = InTransit0, acks = Acks  }) ->
    %% The first message is always readable. It cannot be acked
    %% because if it was, the ack_marker would advance, and the
    %% read_marker would advance as well. It cannot be in_transit,
    %% because if it was, the read_marker would advance to reflect
    %% that as well.
    {NextReadMarker, SeqIdsToRead0} = prepare_read(Num - 1, WriteMarker,
                                                   ReadMarker + 1, InTransit0,
                                                   Acks, []),
    SeqIdsToRead = [ReadMarker|SeqIdsToRead0],
    {Reads, State} = read_from_disk(SeqIdsToRead,
                                    State0#mqistate{ read_marker = NextReadMarker },
                                    []),
    %% We add the messages that have been read to the in_transit list.
    InTransit = lists:foldl(fun({_, SeqId, _, _, _}, Acc) -> [SeqId|Acc] end,
                            InTransit0, Reads),
    {Reads, State#mqistate{ in_transit = InTransit }}.

%% Return when we have found as many messages as requested.
prepare_read(0, _, ReadMarker, _, _, Acc) ->
    {ReadMarker, lists:reverse(Acc)};
%% Return when we have reached the end of messages in the index.
prepare_read(_, WriteMarker, ReadMarker, _, _, Acc)
        when WriteMarker =:= ReadMarker ->
    {ReadMarker, lists:reverse(Acc)};
%% For each seq_id() we may read, we check whether they are in
%% the in_transit list or in the acks list. If they are, we should
%% not read them. Otherwise we add them to the list of messages
%% that we want to read.
prepare_read(Num, WriteMarker, ReadMarker, InTransit, Acks, Acc) ->
    %% We arbitrarily look into the in_transit list first.
    %% It is unknown whether looking into the acks list
    %% first would provide better performance.
    Skip = lists:member(ReadMarker, InTransit)
        orelse lists:member(ReadMarker, Acks),
    case Skip of
        true ->
            prepare_read(Num, WriteMarker, ReadMarker + 1, InTransit, Acks, Acc);
        false ->
            prepare_read(Num - 1, WriteMarker, ReadMarker + 1, InTransit, Acks, [ReadMarker|Acc])
    end.

%% We try to minimize the number of file:read calls when reading from
%% the disk. We find the number of continuous messages, read them all
%% at once, and then repeat the loop.

read_from_disk([], State, Acc) ->
    {lists:reverse(Acc), State};
read_from_disk(SeqIdsToRead0, State0, Acc0) ->
    FirstSeqId = hd(SeqIdsToRead0),
    %% We get the highest continuous seq_id() from the same segment file.
    %% If there are more continuous entries we will read them on the
    %% next loop.
    {LastSeqId, SeqIdsToRead} = highest_continuous_seq_id(SeqIdsToRead0,
                                                          next_segment_boundary(FirstSeqId)),
    ReadSize = (LastSeqId - FirstSeqId + 1) * ?ENTRY_SIZE,
    {Fd, OffsetForSeqId, State} = get_fd(FirstSeqId, State0),
    {ok, _} = file:position(Fd, OffsetForSeqId),
    {ok, EntriesBin} = file:read(Fd, ReadSize),
    %% We cons new entries into the Acc and only reverse it when we
    %% are completely done reading new entries.
    Acc = parse_entries(EntriesBin, FirstSeqId, Acc0),
    read_from_disk(SeqIdsToRead, State, Acc).

get_fd(SeqId, State = #mqistate{ read_write_fds = OpenFds }) ->
    SegmentEntryCount = segment_entry_count(),
    Segment = SeqId div SegmentEntryCount,
    Offset = ?HEADER_SIZE + (SeqId rem SegmentEntryCount) * ?ENTRY_SIZE,
    case OpenFds of
        #{ Segment := Fd } ->
            {Fd, Offset, State};
        _ ->
            {ok, Fd} = file:open(segment_file(Segment, State), [read, write, raw, binary]),
            {Fd, Offset, State#mqistate{ read_write_fds = OpenFds#{ Segment => Fd }}}
    end.

%% When recovering from a dirty shutdown, we may end up reading entries that
%% have already been acked. We do not add them to the Acc in that case, and
%% as a result we may end up returning less messages than initially expected.

parse_entries(<<>>, _, Acc) ->
    Acc;
parse_entries(<< IsAcked:8,
                 IsDelivered:8,
                 _:7, IsPersistent:1,
                 _:8,
                 Id0:128,
                 Size:32/unsigned,
                 Expiry0:64/unsigned,
                 Rest/bits >>, SeqId, Acc) ->
    %% We skip entries that have already been acked. This may only
    %% happen when we recover from a dirty shutdown.
    case IsAcked of
        0 ->
            %% We get the Id binary in two steps because we do not want
            %% to create a sub-binary and keep the larger binary around
            %% in memory.
            Id = <<Id0:128>>,
            Expiry = case Expiry0 of
                0 -> undefined;
                _ -> Expiry0
            end,
            Props = #message_properties{expiry = Expiry, size = Size},
            parse_entries(Rest, SeqId + 1, [{Id, SeqId, Props, IsPersistent =:= 1, IsDelivered =:= 1}|Acc]);
        1 ->
            %% @todo It would be good to keep track of how many "misses"
            %%       we have. We can use it to confirm the correct behavior
            %%       of the module in tests, as well as an occasionally
            %%       useful internal metric. Maybe use counters.
            parse_entries(Rest, SeqId + 1, Acc)
    end.

%% ----
%%
%% Defer to rabbit_queue_index for recovery for the time being.
%% @todo This is most certainly wrong.

start(VHost, DurableQueueNames) ->
    rabbit_queue_index:start(VHost, DurableQueueNames).

stop(VHost) ->
    rabbit_queue_index:stop(VHost).

%% ----
%%
%% These functions either call the normal functions or are no-ops.
%% They relate to specific optimizations of rabbit_queue_index and
%% rabbit_variable_queue.

pre_publish(MsgOrId, SeqId, MsgProps, IsPersistent, _IsDelivered, TargetRamCount, State0) ->
    State = publish(MsgOrId, SeqId, MsgProps, IsPersistent, TargetRamCount, State0),
    %% @todo I don't know what this is but we only want to write if we have never
    %% written before? And we want to write in the right order.
    %% @todo Do something about IsDelivered too? I don't understand this function.
    %% I think it's to allow sending the message to the consumer before writing to the index.
    %% If it is delivered then we need to increase our read_marker. If that doesn't work
    %% then we can always keep a list of delivered messages in memory? -> probably necessary
    State.

%% @todo -spec flush_pre_publish_cache(???, State) -> State when State::mqistate().

flush_pre_publish_cache(_TargetRamCount, State) ->
    State.

-spec sync(State) -> State when State::mqistate().

sync(State) ->
    State.

-spec needs_sync(mqistate()) -> 'false'.

needs_sync(_State) ->
    false.

-spec flush(State) -> State when State::mqistate().

flush(State) ->
    State.

%% See comment in rabbit_queue_index:bounds/1. We do not need to be
%% accurate about these values because they are simply used as lowest
%% and highest possible bounds.

-spec bounds(State) ->
                       {non_neg_integer(), non_neg_integer(), State}
                       when State::mqistate().

%% @todo Implement so that we don't start at the second segment...
%% @todo Second one must be write_marker.
bounds(State) -> % = #mqistate{oldest_segment=Oldest, newest_segment=Newest}) ->
    {0, 0, State}.
%    SegmentEntryCount = segment_entry_count(),
%    {Oldest * SegmentEntryCount, (1 + Newest) * SegmentEntryCount, State}.

%% The next_segment_boundary/1 function is used internally when
%% reading. It should not be called from rabbit_variable_queue.

-spec next_segment_boundary(SeqId) -> SeqId when SeqId::seq_id().

next_segment_boundary(SeqId) ->
    SegmentEntryCount = segment_entry_count(),
    (1 + (SeqId div SegmentEntryCount)) * SegmentEntryCount.

%% ----
%%
%% Internal.

segment_entry_count() ->
    %% @todo Figure out what the best default would be.
    SegmentEntryCount =
        application:get_env(rabbit, modern_queue_index_segment_entry_count, 65536),
    SegmentEntryCount.

erase_index_dir(Dir) ->
    case rabbit_file:is_dir(Dir) of
        true  -> rabbit_file:recursive_delete([Dir]);
        false -> ok
    end.

queue_dir(VHostDir, QueueName) ->
    %% Queue directory is
    %% {node_database_dir}/msg_stores/vhosts/{vhost}/queues/{queue}
    QueueDir = queue_name_to_dir_name(QueueName),
    filename:join([VHostDir, "queues", QueueDir]).

queue_name_to_dir_name(#resource { kind = queue,
                                   virtual_host = VHost,
                                   name = QName }) ->
    <<Num:128>> = erlang:md5(<<"queue", VHost/binary, QName/binary>>),
    rabbit_misc:format("~.36B", [Num]).

segment_file(Segment, #mqistate{ dir = Dir }) ->
    filename:join(Dir, integer_to_list(Segment) ++ ?SEGMENT_EXTENSION).

highest_continuous_seq_id([SeqId1, SeqId2|Tail])
        when (1 + SeqId1) =:= SeqId2 ->
    highest_continuous_seq_id([SeqId2|Tail]);
highest_continuous_seq_id([SeqId|_]) ->
    SeqId.

highest_continuous_seq_id([SeqId|Tail], EndSeqId)
        when (1 + SeqId) =:= EndSeqId ->
    {SeqId, Tail};
highest_continuous_seq_id([SeqId1, SeqId2|Tail], EndSeqId)
        when (1 + SeqId1) =:= SeqId2 ->
    highest_continuous_seq_id([SeqId2|Tail], EndSeqId);
highest_continuous_seq_id([SeqId|Tail], _) ->
    {SeqId, Tail}.
