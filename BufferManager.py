import json
import time

class BufferManager:
    def __init__(self, db_config, disk_manager):
        self.db_config = db_config
        self.disk_manager = disk_manager
        self.bm_policy = dbConfig.bm_policy
        self.buffer_pool = [bytearray([255]) for _ in range(self.db_config.bm_buffercount)]

    def GetPage(self, pageId):
        for buffer in enumerate(self.buffer_pool):
            if buffer[:24] == pageId.FileIdx & buffer[24:48]== pageId.PageIdx:
                buffer[96:120]= time.time()
                return buffer

        for buffer in enumerate(self.buffer_pool):
            if buffer[:24] == 255:
                buffer[:24] = pageId.FileIdx
                buffer[24:48] = pageId.PageIdx
                buffer[48:72] = 0
                buffer[72:96] = 0
                buffer[96:120] = time.time()
                #buffer[120:] = lecture de la page du fichier
                return buffer

        if(bm_policy == "LRU"):
            NBuff = Lru(self.buffer_pool)
            if(NBuff==-1):
                print("ERREUR LRU")
                buffer = null;
            else:
                buffer = self.buffer_pool[NBuff]
                buffer[:24] = pageId.FileIdx
                buffer[24:48] = pageId.PageIdx
                buffer[48:72] = 0
                buffer[72:96] = 0
                buffer[96:120] = time.time()
            return buffer
        
        if(bm_policy == "MRU"):
            NBuff = Mru(self.buffer_pool)
            if(NBuff==-1):
                print("ERREUR MRU")
                buffer = null;
            else:
                buffer = self.buffer_pool[NBuff]
                buffer[:24] = pageId.FileIdx
                buffer[24:48] = pageId.PageIdx
                buffer[48:72] = 0
                buffer[72:96] = 0
                buffer[96:120] = time.time()
            return buffer

    def FlushBuffers(self):
        for buffer in enumerate(self.buffer_pool):
            freepage(buffer,buffer[72:96])

    def lru(self):
        indice = -1
        oldest_time = 0
        for i, page_info in enumerate(self.liste_pages):
            if page_info[48:72] == 0 & time.time() - page_info[72:96] > oldest_time:
                oldest_time = time.time() - page_info[96:120]
                indice = i
        return indice 
    
    def mru(self):
        # Most Recently Used replacement policy
        indice = -1
        newest_time = 99999999999999999999
        for i, page_info in enumerate(self.liste_pages):
            if page_info.get_pin_count() == 0 and time.time() - page_info.get_time() < newest_time:
                newest_time = time.time() - page_info[96:120]
                indice = i
        return indice 

    def FreePage(self, pageId, valdirty):
        for i, buffer in enumerate(self.buffer_pool):
            if buffer[:24] == pageId.FileIdx & buffer[24:48] == pageId.PageIdx :
                if buffer[48:72] == 0 & valdirty == 0:
                    buffer [0:24] = 255
                    buffer [24:48] = 255
                if buffer[48:72] > 0:
                    print("cette page est en cours d'utilisation elle ne peut pas etre supprimer")
                if buffer[48:72] == 0 & valdirty == 1:
                    #we have to write the page calling the write function of the disk manager
                    buffer [0:24] = 255
                    buffer [24:48] = 255
