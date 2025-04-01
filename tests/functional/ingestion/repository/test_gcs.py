import os

from ingestion import repository

from app.common.gcs import FIXTURES


class TestEligibilityFileManagerLocal:
    @staticmethod
    async def test_put_and_get():
        """
        Simple test on local storage
        @return:
        """
        # Given
        bucket = "bucket"
        file_name = "test_file.csv"
        file_content = "random-file-content"
        path = FIXTURES / bucket / file_name
        if path.exists():
            os.remove(path)
        file_manager: repository.EligibilityFileManager = (
            repository.EligibilityFileManager("local-dev")
        )

        # When
        await file_manager.put(file_content, file_name, bucket)
        # Then
        assert path.exists()

        # When
        read = file_manager.get(file_name, bucket)
        # Then
        assert read == file_content
