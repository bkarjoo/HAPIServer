class LockLessQueue(object):

    def __init__(self):
        self.lofls = list()
        self.lofls.append([])
        self.read_list_index = 0
        self.read_sub_index = 0
        self.max_list_size = 10000
        self.max_lag = 0

    def add_item(self, item):
        self.lofls[len(self.lofls)-1].append(item)
        if len(self.lofls[len(self.lofls)-1]) >= self.max_list_size:
            self.lofls.append([])

    # not thread safe
    def read_item(self):
        l = self.lofls[self.read_list_index]
        if self.read_sub_index < len(l):
            self.read_sub_index += 1
            currentLag = self.lag()
            if currentLag > self.max_lag: self.max_lag = currentLag
            return l[self.read_sub_index - 1]
        else:
            # check if there's another list to move to
            if self.read_list_index < len(self.lofls) - 1:
                # clear previous list
                self.lofls[self.read_list_index] = []
                # move to next list
                self.read_list_index += 1
                # reset index

                # return first item of next list

                if len(self.lofls[self.read_list_index]) > 0:
                    self.read_sub_index = 1
                    return self.lofls[self.read_list_index][0]
                else:
                    raise LockLessQueue.EmptyList('List is empty.')
            else:
                raise LockLessQueue.EmptyList('List is empty.')


    def __len__(self):
        sum = 0
        for l in self.lofls:
            sum += len(l)
        return sum

    def lag(self):
        writes = (len(self.lofls) - 1) * self.max_list_size + len(self.lofls[len(self.lofls)-1])
        reads = self.read_list_index * self.max_list_size + self.read_sub_index
        return writes - reads


    class EmptyList(Exception):
        pass
