from dagster import asset, RetryPolicy


def create_schoolmint_grow_asset(asset_name, query_name):
    """
    Take in customer instance name
    Return list json objects with response's data property
    """

    @asset(
        name=asset_name,
        group_name="schoolmint_grow",
        io_manager_key="gcs_io_manager",
        metadata={"path": f"schoolmint_grow_api/{asset_name}"},
        required_resource_keys={"schoolmint_grow_client"},
        retry_policy=RetryPolicy(max_retries=2),
    )
    def schoolmint_grow_asset(context):
        """
        retrieve records from the schoolmint grow api
        return a json object of the result
        """
        responses = context.resources.schoolmint_grow_client.get_data(query_name)

        result = []
        for record in responses["data"]:
            result.append({"data": record})

        return result

    return schoolmint_grow_asset
