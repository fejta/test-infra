
def autogo_dependencies():
  if "io_bazel_rules_go" not in native.existing_rules():
    print("Creating @io_bazel_rules_go...")
    native.git_repository(
        name = "io_bazel_rules_go",
        commit = "6b39964af66c98580be4c5ac6cf1d243332f78e4",
        remote = "https://github.com/bazelbuild/rules_go.git")
  else:
    print("Detected @io_bazel_rules_go")
