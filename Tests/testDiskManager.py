import os
import shutil
import unittest

# Import the DiskManager and PageId classes from DiskManager.py
from DiskManager import DiskManager
from pageId import PageId

# Mock dbc class to simulate database configuration
class MockDBC:
    def __init__(self, dbpath, pageSize, dm_maxfilesize):
        self.dbpath = dbpath
        self.pageSize = pageSize
        self.dm_maxfilesize = dm_maxfilesize

class TestDiskManager(unittest.TestCase):
    def setUp(self):
        # Set up a mock database configuration
        self.dbc = MockDBC(dbpath="test_db", pageSize=4096, dm_maxfilesize=4)
        # Initialize the DiskManager with the mock dbc
        self.disk_manager = DiskManager(self.dbc)
        # Ensure the test database directory is clean
        if os.path.exists(self.dbc.dbpath):
            shutil.rmtree(self.dbc.dbpath)
        os.makedirs(os.path.join(self.dbc.dbpath, "BinData"), exist_ok=True)

    def tearDown(self):
        # Clean up the test database directory after tests
        if os.path.exists(self.dbc.dbpath):
            shutil.rmtree(self.dbc.dbpath)

    def test_alloc_page_creates_files_and_pages_correctly(self):
        allocated_pages = []
        total_pages_to_allocate = 10  # For testing multiple file creations

        for _ in range(total_pages_to_allocate):
            page_id = self.disk_manager.AllocPage()
            self.assertIsNotNone(page_id, "AllocPage returned None")
            allocated_pages.append(page_id)

        # Verify that the correct number of files were created
        data_dir = os.path.join(self.dbc.dbpath, "BinData")
        files = [f for f in os.listdir(data_dir) if f.startswith('F') and f.endswith('.rsdb')]
        expected_files = (total_pages_to_allocate - 1) // self.dbc.dm_maxfilesize + 1
        self.assertEqual(len(files), expected_files, "Incorrect number of data files created")

        # Verify that each file has no more than dm_maxfilesize pages
        for file_idx in range(expected_files):
            file_name = f"F{file_idx}.rsdb"
            file_path = os.path.join(data_dir, file_name)
            self.assertTrue(os.path.exists(file_path), f"Data file {file_name} does not exist")
            file_size = os.path.getsize(file_path)
            pages_in_file = file_size // self.dbc.pageSize
            if file_idx < expected_files - 1:
                expected_pages = self.dbc.dm_maxfilesize
            else:
                expected_pages = total_pages_to_allocate % self.dbc.dm_maxfilesize
                if expected_pages == 0:
                    expected_pages = self.dbc.dm_maxfilesize
            self.assertEqual(pages_in_file, expected_pages, f"Incorrect number of pages in {file_name}")
            
            

  
    def test_alloc_page_reuses_deallocated_pages(self):
        # Allocate and deallocate pages
        page_ids = [self.disk_manager.AllocPage() for _ in range(5)]
        for page_id in page_ids:
            self.disk_manager.DeallocPage(page_id)

        # Allocate pages again and ensure the same pages are reused
        reused_page_ids = [self.disk_manager.AllocPage() for _ in range(5)]
        self.assertEqual(page_ids, reused_page_ids, "Deallocated pages were not reused")

    def test_alloc_page_handles_no_free_pages(self):
        # Ensure no free pages
        self.disk_manager.free_pages = []
        self.disk_manager.SaveState()

        # Allocate a page
        page_id = self.disk_manager.AllocPage()
        self.assertIsNotNone(page_id, "AllocPage returned None when no free pages")

    def test_alloc_page_creates_new_file_when_last_is_full(self):
        # Fill up the first file
        for _ in range(self.dbc.dm_maxfilesize):
            page_id = self.disk_manager.AllocPage()
            self.assertEqual(page_id.FileIdx, 0, "Page allocated in wrong file")

        # Next allocation should create a new file
        new_page_id = self.disk_manager.AllocPage()
        self.assertEqual(new_page_id.FileIdx, 1, "New file was not created when last was full")
        self.assertEqual(new_page_id.PageIdx, 0, "First page in new file should have PageIdx 0")

if __name__ == '__main__':
    unittest.main()
