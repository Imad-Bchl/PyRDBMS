import json

class BufferManager:
    def __init__(self, db_config, disk_manager):
        self.db_config = db_config
        self.disk_manager = disk_manager
        self.buffer_pool = []
        self.replacement_policy=self.db_config.bm_policy
        self.buffer_capacity = self.db_config.bm_buffercount

    def GetPage(self, pageId):
        for i, (pid, buffer) in enumerate(self.buffer_pool):
            if pid == pageId:
                self.buffer_pool.append(self.buffer_pool.pop(i))
                return buffer
    #If the page is not in the buffer pool 
    # the buffer manager reads the page from the disk manager and adds it to the buffer pool.
    
        buffer = bytearray(self.db_config.pageSize)
        self.disk_manager.ReadPage(pageId, buffer)
        
        
        
        if len(self.buffer_pool) >= self.buffer_capacity:
            if self.replacement_policy == "LRU":
                self.buffer_pool.pop(0)
            elif self.replacement_policy == "MRU":
                self.buffer_pool.pop(-1)
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
            
    def SetCurrentReplacementPolicy(self, policy):
        self.replacement_policy = policy
        
    def FlushBuffers(self):
        for pid, buffer, pin_count, dirty_flag in self.buffer_pool:
            if dirty_flag:
                self.disk_manager.WritePage(pid, buffer)
                dirty_flag = False
                self.buffer_pool[i] = (pid, buffer, pin_count, dirty_flag)
        return