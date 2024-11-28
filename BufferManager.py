import time
import datetime
import struct
from DiskManager import DiskManager
from pageId import PageId

class BufferManager:
    def __init__(self, dbConfig, disk_manager):
        self.db_config = dbConfig
        self.disk_manager = disk_manager
        self.bm_policy = dbConfig.bm_policy
        self.buffer_pool = [bytearray([255]*20) for _ in range(self.db_config.bm_buffercount)]
        new_epoch = datetime.datetime(2024, 10, 1)
        self.epoch_difference = (new_epoch - datetime.datetime(1970, 1, 1)).total_seconds()

    def GetPage(self, pageId):
        for i,buffer in enumerate(self.buffer_pool):
            if struct.unpack('i', buffer[:4])[0] == pageId.FileIdx and struct.unpack('i', buffer[4:8])[0] == pageId.PageIdx:
                buffer[16:20]= (time.time() - self.epoch_difference)
                return i

        for i,buffer in enumerate(self.buffer_pool):
            if struct.unpack('i', buffer[:4])[0] == -1:
                buffer[:4] = struct.pack('i', pageId.FileIdx) 
                buffer[4:8] = struct.pack('i', pageId.PageIdx)
                buffer[8:12] = struct.pack('i', 0) 
                buffer[12:16] = struct.pack('i', 0) 
                buffer[16:20] = struct.pack('f', float((time.time() - self.epoch_difference)*1000)) 
                buffer1 = self.disk_manager.ReadPage(pageId)
                buffer[20:] = buffer1
                #buffer[120:] = lecture de la page du fichier)
                return i

        if(self.bm_policy == "LRU"):
            NBuff = self.lru()
            if(NBuff==-1):
                print("ERREUR LRU")
                buffer = bytearray()
            else:
                buffer = self.buffer_pool[NBuff]
                buffer[:4] = struct.pack('i', pageId.FileIdx) 
                buffer[4:8] = struct.pack('i', pageId.PageIdx)
                buffer[8:12] = struct.pack('i', 0) 
                buffer[12:16] = struct.pack('i', 0) 
                buffer[16:20] = struct.pack('f', float((time.time() - self.epoch_difference)*1000))  
                buffer1 = self.disk_manager.ReadPage(pageId)
                buffer[20:] = buffer1
            return NBuff
        
        if(self.bm_policy == "MRU"):
            NBuff = self.mru()
            if(NBuff==-1):
                print("ERREUR MRU")
                buffer = bytearray()
            else:
                buffer = self.buffer_pool[NBuff]
                buffer[:4] = struct.pack('i', pageId.FileIdx) 
                buffer[4:8] = struct.pack('i', pageId.PageIdx)
                buffer[8:12] = struct.pack('i', 0) 
                buffer[12:16] = struct.pack('i', 0) 
                buffer[16:20] = struct.pack('f', float((time.time() - self.epoch_difference)*1000))   
                buffer1 = self.disk_manager.ReadPage(pageId)
                buffer[20:] = buffer1
            return NBuff

    def FlushBuffers(self):
        for buffer in enumerate(self.buffer_pool):
            self.freepage(buffer,buffer[12:16])

    def lru(self):
        indice = -1
        oldest_time = 0
        time1 = int((time.time() - self.epoch_difference)*1000)
        for i in range(self.db_config.bm_buffercount):
            #print(time1 - struct.unpack("i",page_info[12:16])[0])
            time2 = time1 - struct.unpack("f",self.buffer_pool[i][16:20])[0]
            if struct.unpack('i', self.buffer_pool[i][8:12])[0] == 0 and  time2 > oldest_time:
                oldest_time = time2
                indice = i
        return indice 
    
    def mru(self):
        # Most Recently Used replacement policy
        indice = -1
        newest_time = 99999999999999999999
        time1 = int((time.time() - self.epoch_difference)*1000)
        for i in range(self.db_config.bm_buffercount):
            time2 = time1 - struct.unpack("f",self.buffer_pool[i][16:20])[0]
            if struct.unpack('i', self.buffer_pool[i][8:12])[0] == 0 and  time2 < newest_time:
                newest_time = time2
                indice = i
        return indice 

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

