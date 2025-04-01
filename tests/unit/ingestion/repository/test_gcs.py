from unittest import mock

import pytest
from ingestion import repository

pytestmark = pytest.mark.asyncio


class TestEligibilityFileManager:
    file_name = "directory/file.csv"
    bucket_name = "bucket"

    @staticmethod
    async def test_get(file_manager: repository.EligibilityFileManager):
        # Given
        encoded: bytes = "some string".encode("ASCII")
        file_manager.crypto.decrypt.return_value = (encoded, {})

        # When
        with mock.patch(
            "ingestion.repository.gcs.ddtrace.tracer.current_span"
        ) as mock_current_span, mock.patch(
            "app.common.gcs.AsyncBlob.download"
        ) as mock_download:
            mock_current_span.return_value = mock.MagicMock()
            mock_download.return_value = encoded

            retrieved: bytes = await file_manager.get(
                name=TestEligibilityFileManager.file_name,
                bucket_name=TestEligibilityFileManager.bucket_name,
            )

        assert retrieved == encoded

    @staticmethod
    async def test_get_missing_blob(file_manager: repository.EligibilityFileManager):
        # Given
        file_manager.storage.get_blob.return_value = None

        # When
        retrieved: bytes = await file_manager.get(
            name=TestEligibilityFileManager.file_name,
            bucket_name=TestEligibilityFileManager.bucket_name,
        )

        assert retrieved is None

    @staticmethod
    async def test_get_unencrypted(file_manager: repository.EligibilityFileManager):
        # Given
        encoded: bytes = "some string".encode("ASCII")
        file_manager.crypto.decrypt.return_value = (encoded, {})
        file_manager.encrypted = False

        # When
        with mock.patch(
            "ingestion.repository.gcs.ddtrace.tracer.current_span"
        ) as mock_current_span, mock.patch(
            "app.common.gcs.AsyncBlob.download"
        ) as mock_download:
            mock_current_span.return_value = mock.MagicMock()
            mock_download.return_value = encoded

            await file_manager.get(
                name=TestEligibilityFileManager.file_name,
                bucket_name=TestEligibilityFileManager.bucket_name,
            )

        file_manager.crypto.decrypt.assert_not_called()

    @staticmethod
    async def test_put_unencrypted(file_manager: repository.EligibilityFileManager):
        # Give
        data: bytes = "plained content".encode("ASCII")
        file_manager.encrypted = False

        # When
        save_blob = file_manager.storage.save_blob
        await file_manager.put(
            data,
            name=TestEligibilityFileManager.file_name,
            bucket_name=TestEligibilityFileManager.bucket_name,
        )
        save_blob.assert_called_with(
            data=data,
            name=TestEligibilityFileManager.file_name,
            bucket_name=TestEligibilityFileManager.bucket_name,
        )

    @staticmethod
    async def test_put_encrypted(file_manager: repository.EligibilityFileManager):
        encrypted: bytes = "encrypted content".encode("ASCII")
        hash = "mocked hash"
        metadata = {"key1": "val1", "key2": "val2"}

        file_manager.encrypted = True
        file_manager.crypto.encrypt.return_value = (hash, encrypted, metadata)

        save_blob = file_manager.storage.save_blob
        await file_manager.put(
            "any content",
            name=TestEligibilityFileManager.file_name,
            bucket_name=TestEligibilityFileManager.bucket_name,
            kek_name="kek",
            signing_key_name="sig",
        )

        save_blob.assert_called_with(
            encrypted,
            TestEligibilityFileManager.file_name,
            TestEligibilityFileManager.bucket_name,
            content_type="application/octet-stream",
            key1="val1",
            key2="val2",
        )

    @staticmethod
    @pytest.mark.parametrize(
        argnames="kek,sig_key",
        argvalues=[
            (None, None),
            (None, "sig_key"),
            ("kek", None),
        ],
        ids=[
            "no_kek_no_sig_key",
            "no_kek",
            "no_sig_key",
        ],
    )
    async def test_put_encrypted_error_when_no_kek_sig(
        file_manager: repository.EligibilityFileManager, kek, sig_key
    ):
        data: bytes = "plained content".encode("ASCII")
        file_manager.encrypted = True
        save_blob = file_manager.storage.save_blob
        encrypt = file_manager.crypto.encrypt
        await file_manager.put(
            data,
            name=TestEligibilityFileManager.file_name,
            bucket_name=TestEligibilityFileManager.bucket_name,
            kek_name=kek,
            signing_key_name=sig_key,
        )
        encrypt.assert_not_called()
        save_blob.assert_called_with(
            data=data,
            name=TestEligibilityFileManager.file_name,
            bucket_name=TestEligibilityFileManager.bucket_name,
        )
