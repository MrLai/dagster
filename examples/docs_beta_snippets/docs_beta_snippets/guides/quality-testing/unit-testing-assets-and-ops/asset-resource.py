import mock
from dagster_aws.s3 import S3FileHandle, S3FileManager

import dagster as dg


@dg.asset
def loaded_file(file_manager: S3FileManager) -> str:
    return file_manager.read_data(S3FileHandle("bucket", "path.txt"))


# highlight-start
def test_file() -> None:
    mocked_resource = mock.Mock(spec=S3FileManager)
    mocked_resource.read_data.return_value = "contents"

    assert loaded_file(mocked_resource) == "contents"
    assert mocked_resource.read_data.called_once_with(
        S3FileHandle("bucket", "path.txt")
    )
    # highlight-end
