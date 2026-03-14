from api.pipeline_api import describe_contract, implementation_status


def test_pipeline_api_contract_documents_expected_endpoints():
    contract = describe_contract()
    paths = {endpoint.path for endpoint in contract}

    assert implementation_status() == "planned"
    assert paths == {
        "/projects",
        "/projects/{project}/summary",
        "/projects/{project}/marts",
        "/projects/{project}/marts/{mart_name}",
        "/projects/{project}/run",
    }
