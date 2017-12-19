import math
import numpy as np


WIDTH=15
MIN_COUNT=6
ITERATIONS=25
DOWNSAMPLE=False

INDICES=np.indices((256,256))

class MShift(object):
    #
    # PUBLIC METHODS
    #
    def __init__(self,
            data,
            width=WIDTH,
            min_count=MIN_COUNT,
            iterations=ITERATIONS,
            downsample=DOWNSAMPLE):
        self.data=data
        self.width=width/100.0
        self.min_count=min_count
        self.iterations=iterations
        # TODO: Implement wbi and dsmpl
        self.downsample=downsample
        self._init_properties()


    def input_data(self):
        if self._input_data is None:
            self._input_data=np.dstack((INDICES[0],INDICES[1],self.data))
            self._input_data=self._input_data.reshape(256*256,-1)
            self._input_data=self._input_data[self._input_data[:,-1]>1]
        return self._input_data


    def clustered_data(self):
        if self._clusters is None:   
            self._clusters=np.delete(self.input_data().copy(),-1,axis=1)
            values=self.input_data().copy()[:,-1]
            for n in range(self.iterations):
                for i, x in enumerate(self._clusters):
                    dist = np.sqrt(((x-self._clusters)**2).sum(1))
                    weight = self._gaussian(dist)
                    self._clusters[i] = (np.expand_dims(weight,1)*self._clusters).sum(0) / weight.sum()
        return self._clusters


    def clusters(self):
        points=[]
        if len(self.clustered_data()):
            xu,count=np.unique(self.clustered_data(),axis=0,return_counts=True)
            for t,cnt in zip(xu,count): 
                if cnt>=self.min_count: 
                    points.append(t.tolist())
        return np.array(points).astype(int)


    #
    # INTERNAL 
    #
    def _init_properties(self):
        self._input_data=None
        self._clusters=None


    def _gaussian(self,d):
        return np.exp(-0.5*((d/self.width))**2) / (self.width*math.sqrt(2*math.pi))





