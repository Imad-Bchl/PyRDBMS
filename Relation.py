from os import rename
import struct
import ColInfo

class Relation:
     
    def __init__(self,relationName,nbCollumn,colInfoList,tailleVar):
        self.relationName=relationName
        self.nbCollumn=nbCollumn
        self.col_info_list = colInfoList
        self.tailleVar = tailleVar

    def getNbCollumn(self):
        return self.nbCollumn
    
    def getCol_info_list(self):
        return self.col_info_list

    def writeRecordToBuffer(self, record, buff, pos):
        if self.tailleVar == False :
            posRel = pos
            index = 0
            for col in self.col_info_list:
                match col.colType:
                    case "INT":
                        buff[posRel:posRel+4] = struct.pack('i', record[index])
                        posRel += 4
                        index += 1
                    case "FLOAT":
                        buff[posRel:posRel+4] = struct.pack('f', record[index])
                        posRel += 4
                        index += 1
                    case "CHAR(T)":
                        # il faut trouver un moyen de savoir la taille T du char
                        index +=1
                    case "VARCHAR(T)":
                        # il faut trouver un moyen de savoir la taille T du char
                        index += 1

            return posRel-pos
        else:
            posRel = pos #la on vas incrire les tailles des cols
            posRel1 = pos + self.nbCollumn*4 #la c'est les valeurs des cols
            index = 0
            for col in self.col_info_list:
                match col.colType:
                    case "INT":
                        buff[posRel:posRel+4] = struct.pack('i', 4)
                        buff[posRel1:posRel1+4] = struct.pack('i', record[index])
                        posRel += 4
                        posRel1 += 4
                        index += 1
                    case "FLOAT":
                        buff[posRel:posRel+4] = struct.pack('i', 4)
                        buff[posRel:posRel+4] = struct.pack('f', record[index])
                        posRel += 4
                        posRel1 += 4
                        index += 1
                    case "CHAR(T)":
                        # il faut trouver un moyen de savoir la taille T du char
                        posRel += 4
                        posRel1 += T #si la chaine est moins longue que T on vas faire un bourage avec des $
                        index +=1
                    case "VARCHAR(T)":
                        # il faut trouver un moyen de savoir la taille T du char
                        posRel += 4
                        posRel1 += T #taille reel
                        index += 1
            return posRel1-pos
                
def readFromBuffer(self, record, buff, pos):
        if self.tailleVar == False :
            posRel = pos
            index = 0
            for col in self.col_info_list:
                match col.colType:
                    case "INT":
                        record[index] = struct.unpack('i', buff[posRel:posRel+4])[0]
                        posRel += 4
                        index += 1
                    case "FLOAT":
                        record[index] = struct.unpack('i', buff[posRel:posRel+4])[0]
                        posRel += 4
                        index += 1
                    case "CHAR(T)":
                        # il faut trouver un moyen de savoir la taille T du char
                        index +=1
                    case "VARCHAR(T)":
                        # il faut trouver un moyen de savoir la taille T du char
                        index += 1

            return posRel-pos
        else:
            posRel = pos #la on vas incrire les tailles des cols
            posRel1 = pos + self.nbCollumn*4 #la c'est les valeurs des cols
            index = 0
            for col in self.col_info_list:
                match col.colType:
                    case "INT":
                        record[index] = struct.unpack('i', buff[posRel1:posRel1+4])[0]
                        posRel += 4
                        posRel1 += 4
                        index += 1
                    case "FLOAT":
                        record[index] = struct.unpack('i', buff[posRel1:posRel1+4])[0]
                        posRel += 4
                        posRel1 += 4
                        index += 1
                    case "CHAR(T)":
                        taille = struct.unpack('i', buff[posRel:posRel+4])[0]
                        text = struct.unpack(f'{taille}s', buff[posRel1:posRel1+taille])[0]
                        for i in range(taille):
                            if text[i] != '$':
                                val = val + text[i]
                        record[index] = val
                        posRel += 4
                        posRel1 += posRel1+taille
                        index += 1
                    case "VARCHAR(T)":
                        taille = struct.unpack('i', buff[posRel:posRel+4])[0]
                        record[index] = struct.unpack(f'{taille}s', buff[posRel1:posRel1+taille])[0]
                        posRel += 4
                        posRel1 += posRel1+taille
                        index += 1
            return posRel1-pos