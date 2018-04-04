load("@io_bazel_rules_go//go:def.bzl", "go_rules_dependencies", "go_register_toolchains")

def _impl(repo_ctx):
  print("Generating rules for %s..." % repo_ctx.attr.prefix)
  prefix = repo_ctx.attr.prefix
  _exec(repo_ctx, ["touch", repo_ctx.path("BUILD.bazel")])
  _exec(repo_ctx,
    ["/Users/fejta/hi", repo_ctx.path(repo_ctx.attr._workspace).dirname, repo_ctx.path(".")], quiet=False)
  _exec(repo_ctx,
    ["/Users/fejta/gazelle", "--repo_root", repo_ctx.path(""), "--go_prefix", repo_ctx.attr.prefix], quiet=False)

_autogo_generate = repository_rule(
    implementation=_impl,
    local=True,
    attrs={
        "prefix": attr.string(mandatory=True),
        "_workspace": attr.label(allow_single_file=True, default="@//:WORKSPACE")}
)

def autogo_generate(*a, **kw):
  go_rules_dependencies()
  go_register_toolchains()
  _autogo_generate(*a, **kw)

def _exec(repo_ctx, cmd, *a, **kw):
  ret = repo_ctx.execute(cmd, *a, **kw)
  if ret.return_code:
    fail([cmd, ret.return_code, ret.stdout, ret.stderr])

