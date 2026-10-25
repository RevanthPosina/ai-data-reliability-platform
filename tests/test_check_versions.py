import check_versions


def test_design_doc_matches_versions_env() -> None:
    assert check_versions.main() == 0
