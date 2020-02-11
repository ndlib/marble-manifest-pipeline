import unittest
from finalize.finalizeStep import FinalizeStep

test_id = "sample"

test_config = {
    "process-bucket": "mybucket",
    "process-bucket-read-basepath": f"process/{test_id}/images",
    "image-server-bucket": "anotherbucket"
}


class TestFinalize(unittest.TestCase):

    def test_image_cleanup(self):
        step = FinalizeStep(test_id, test_config)
        bucket = test_config["process-bucket"]
        path = test_config["process-bucket-read-basepath"]
        images = set(["img1.tif", "img2.tif", "img3.tif", "img4.tif"])
        deleted_images = step._delete_obsolete_pyramids(bucket, path, images, local='y')
        self.assertEqual(images, deleted_images)
