load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

http_archive(
    name = "bazel_skylib",
    sha256 = "af87959afe497dc8dfd4c6cb66e1279cb98ccc84284619ebfec27d9c09a903de",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/1.2.0/bazel-skylib-1.2.0.tar.gz",
        "https://github.com/bazelbuild/bazel-skylib/releases/download/1.2.0/bazel-skylib-1.2.0.tar.gz",
    ],
)

load("@bazel_skylib//:workspace.bzl", "bazel_skylib_workspace")

bazel_skylib_workspace()

http_archive(
    name = "rules_pkg",
    sha256 = "a89e203d3cf264e564fcb96b6e06dd70bc0557356eb48400ce4b5d97c2c3720d",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/0.5.1/rules_pkg-0.5.1.tar.gz",
        "https://github.com/bazelbuild/rules_pkg/releases/download/0.5.1/rules_pkg-0.5.1.tar.gz",
    ],
)

load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")

rules_pkg_dependencies()

http_archive(
    name = "io_bazel_rules_docker",
    sha256 = "b1e80761a8a8243d03ebca8845e9cc1ba6c82ce7c5179ce2b295cd36f7e394bf",
    urls = ["https://github.com/bazelbuild/rules_docker/releases/download/v0.25.0/rules_docker-v0.25.0.tar.gz"],
)

load(
    "@io_bazel_rules_docker//repositories:repositories.bzl",
    container_repositories = "repositories",
)

container_repositories()

load("@io_bazel_rules_docker//repositories:deps.bzl", container_deps = "deps")

container_deps()

load(
    "@io_bazel_rules_docker//container:container.bzl",
    "container_pull",
)

container_pull(
    name = "ubuntu2004",
    registry = "index.docker.io",
    repository = "pivotalrabbitmq/ubuntu",
    tag = "20.04",
)

http_file(
    name = "openssl-1.1.1g",
    downloaded_file_path = "openssl-1.1.1g.tar.gz",
    sha256 = "ddb04774f1e32f0c49751e21b67216ac87852ceb056b75209af2443400636d46",
    urls = ["https://www.openssl.org/source/openssl-1.1.1g.tar.gz"],
)

http_file(
    name = "otp_src_23",
    downloaded_file_path = "OTP-23.3.4.16.tar.gz",
    sha256 = "ff091e8a2b3e6350b890d37123444474cf49754f6add0ccee17582858ee464e7",
    urls = ["https://github.com/erlang/otp/archive/OTP-23.3.4.16.tar.gz"],
)

http_file(
    name = "otp_src_24",
    downloaded_file_path = "OTP-24.3.4.5.tar.gz",
    sha256 = "4831e329ed61ee49f5a5a5e89ad52fd97468325654f17a1892c5664ea4b490f5",
    urls = ["https://github.com/erlang/otp/archive/OTP-24.3.4.5.tar.gz"],
)

http_file(
    name = "otp_src_25_0",
    downloaded_file_path = "OTP-25.0.4.tar.gz",
    sha256 = "05878cb51a64b33c86836b12a21903075c300409b609ad5e941ddb0feb8c2120",
    urls = ["https://github.com/erlang/otp/archive/OTP-25.0.4.tar.gz"],
)

http_file(
    name = "otp_src_25_1",
    downloaded_file_path = "OTP-25.1.tar.gz",
    sha256 = "e00b2e02350688ee4ac83c41ec25c210774fe73b7f806860c46b185457ae135e",
    urls = ["https://github.com/erlang/otp/archive/OTP-25.1.tar.gz"],
)

http_archive(
    name = "io_buildbuddy_buildbuddy_toolchain",
    sha256 = "a2a5cccec251211e2221b1587af2ce43c36d32a42f5d881737db3b546a536510",
    strip_prefix = "buildbuddy-toolchain-829c8a574f706de5c96c54ca310f139f4acda7dd",
    urls = ["https://github.com/buildbuddy-io/buildbuddy-toolchain/archive/829c8a574f706de5c96c54ca310f139f4acda7dd.tar.gz"],
)

load("@io_buildbuddy_buildbuddy_toolchain//:deps.bzl", "buildbuddy_deps")

buildbuddy_deps()

load("@io_buildbuddy_buildbuddy_toolchain//:rules.bzl", "buildbuddy")

buildbuddy(
    name = "buildbuddy_toolchain",
    llvm = True,
)

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

git_repository(
    name = "rbe",
    branch = "linux-rbe",
    remote = "https://github.com/rabbitmq/rbe-erlang-platform.git",
)

git_repository(
    name = "rules_erlang",
    remote = "https://github.com/rabbitmq/rules_erlang.git",
    tag = "3.7.2",
)

load(
    "@rules_erlang//:rules_erlang.bzl",
    "erlang_config",
    "internal_erlang_from_github_release",
    "internal_erlang_from_http_archive",
    "rules_erlang_dependencies",
)

erlang_config(
    internal_erlang_configs = [
        internal_erlang_from_github_release(
            name = "23",
            sha256 = "e3ecb3ac2cc549ab90cd9f8921eaebc8613f4d5c89972a3987e5a762d5a2df08",
            version = "23.3.4.16",
        ),
        internal_erlang_from_github_release(
            name = "24",
            sha256 = "0b57d49e62958350676e8f32a39008d420dca4bc20f2d7e38c0671ab2ba62f14",
            version = "24.3.4.5",
        ),
        internal_erlang_from_github_release(
            name = "25_0",
            sha256 = "8fc707f92a124b2aeb0f65dcf9ac8e27b2a305e7bcc4cc1b2fdf770eec0165bf",
            version = "25.0.4",
        ),
        internal_erlang_from_github_release(
            name = "25_1",
            sha256 = "a5ea27c1e07511a84bdd869c37f5e254f198c1cecf68ee9c8fedd23010750c31",
            version = "25.1",
        ),
        internal_erlang_from_http_archive(
            name = "git_master",
            strip_prefix = "otp-master",
            url = "https://github.com/erlang/otp/archive/refs/heads/master.tar.gz",
            version = "26",
        ),
    ],
)

rules_erlang_dependencies()

load("@erlang_config//:defaults.bzl", "register_defaults")

register_defaults()

load(
    "//bazel/elixir:elixir.bzl",
    "elixir_config",
    "internal_elixir_from_github_release",
)

elixir_config(
    internal_elixir_configs = [
        internal_elixir_from_github_release(
            name = "1_10",
            sha256 = "8518c78f43fe36315dbe0d623823c2c1b7a025c114f3f4adbb48e04ef63f1d9f",
            version = "1.10.4",
        ),
        internal_elixir_from_github_release(
            name = "1_12",
            sha256 = "c5affa97defafa1fd89c81656464d61da8f76ccfec2ea80c8a528decd5cb04ad",
            version = "1.12.3",
        ),
        internal_elixir_from_github_release(
            name = "1_13",
            sha256 = "95daf2dd3052e6ca7d4d849457eaaba09de52d65ca38d6933c65bc1cdf6b8579",
            version = "1.13.4",
        ),
        internal_elixir_from_github_release(
            name = "1_14",
            sha256 = "ac129e266a1e04cdc389551843ec3dbdf36086bb2174d3d7e7936e820735003b",
            version = "1.14.0",
        ),
    ],
    rabbitmq_server_workspace = "@",
)

load(
    "@elixir_config//:defaults.bzl",
    register_elixir_defaults = "register_defaults",
)

register_elixir_defaults()

load("//:workspace_helpers.bzl", "rabbitmq_external_deps")

rabbitmq_external_deps(rabbitmq_workspace = "@")

load("//deps/amqp10_client:activemq.bzl", "activemq_archive")

activemq_archive()

load("//bazel/bzlmod:secondary_umbrella.bzl", "secondary_umbrella")

secondary_umbrella()
