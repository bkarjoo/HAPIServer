from threading import Lock

class Node:
    def __init__(self,initdata):
        self.data = initdata
        self.next = None

    def getData(self):
        return self.data

    def getNext(self):
        return self.next

    def setNext(self,newnext):
        self.next = newnext

class LockLessQueue:
    def __init__(self):
        self.head = None
        self.end = None
        self.lock = Lock()
        self.size = 0
        self.max_lag = 0

    def isEmpty(self):
        return self.head == None

    def add_item(self, item):
        # adds to the end
        with self.lock:
            temp = Node(item)
            if self.end:
                self.end.setNext(temp)
                self.end = temp
            else:
                self.end = temp
                self.head = temp
            self.size += 1
            if self.size > self.max_lag: self.max_lag = self.size

    def read_item(self):
        # remove item
        with self.lock:
            if self.head:
                data = self.head.getData()
                self.head = self.head.getNext()
                self.size -= 1
                if self.size == 0:
                    self.end = None
                return data
            else:
                raise LockLessQueue.EmptyList('List is empty.')

    def __len__(self):
        return self.size

    def lag(self):
        return self.size

    class EmptyList(Exception):
        pass

# class LockLessQueue(object):
#
#     def __init__(self):
#         self.lofls = list()
#         self.lofls.append([])
#         self.read_list_index = 0
#         self.read_sub_index = 0
#         self.max_list_size = 10000
#         self.max_lag = 0
#
#     def add_item(self, item):
#         self.lofls[len(self.lofls)-1].append(item)
#         if len(self.lofls[len(self.lofls)-1]) >= self.max_list_size:
#             self.lofls.append([])
#
#     # not thread safe
#     def read_item(self):
#         l = self.lofls[self.read_list_index]
#         if self.read_sub_index < len(l):
#             self.read_sub_index += 1
#             currentLag = self.lag()
#             if currentLag > self.max_lag: self.max_lag = currentLag
#             return l[self.read_sub_index - 1]
#         else:
#             # check if there's another list to move to
#             if self.read_list_index < len(self.lofls) - 1:
#                 # clear previous list
#                 self.lofls[self.read_list_index] = []
#                 # move to next list
#                 self.read_list_index += 1
#                 # reset index
#
#                 # return first item of next list
#
#                 if len(self.lofls[self.read_list_index]) > 0:
#                     self.read_sub_index = 1
#                     return self.lofls[self.read_list_index][0]
#                 else:
#                     raise LockLessQueue.EmptyList('List is empty.')
#             else:
#                 raise LockLessQueue.EmptyList('List is empty.')
#
#
#     def __len__(self):
#         sum = 0
#         for l in self.lofls:
#             sum += len(l)
#         return sum
#
#     def lag(self):
#         writes = (len(self.lofls) - 1) * self.max_list_size + len(self.lofls[len(self.lofls)-1])
#         reads = self.read_list_index * self.max_list_size + self.read_sub_index
#         return writes - reads
#
#
#     class EmptyList(Exception):
#         pass
