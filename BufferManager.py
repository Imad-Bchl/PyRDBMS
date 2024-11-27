import json

class BufferManager:
    def __init__(self, db_config, disk_manager):
        self.db_config = db_config
        self.disk_manager = disk_manager
        self.buffer_pool = []

    def GetPage(self, pageId):
        for i, (pid, buffer) in enumerate(self.buffer_pool):
            if pid == pageId:
                self.buffer_pool.append(self.buffer_pool.pop(i))
                return buffer
    #If the page is not in the buffer pool 
    # the buffer manager reads the page from the disk manager and adds it to the buffer pool.
    
        buffer = bytearray(self.db_config.pageSize)
        self.disk_manager.ReadPage(pageId, buffer)
        #ToDo : Implement the buffer replacement policy and buffer capacity 
        if len(self.buffer_pool) >= self.buffer_capacity:
            self.buffer_pool.pop(0)
        self.buffer_pool.append((pageId, buffer))

        return buffer

    def FreePage(self, pageId, valdirty):
        for i, (pid, buffer, pin_count, dirty_flag) in enumerate(self.buffer_pool):
            if pid == pageId:
                if pin_count > 0:
                    pin_count -= 1
                else:
                    print("Erreur : pin_count déjà à zéro !")

                if valdirty:
                    dirty_flag = True

                self.buffer_pool[i] = (pid, buffer, pin_count, dirty_flag)
                return 

