import math
import numpy as np


WIDTH=15
MIN_COUNT=6
INTENSITY_THRESHOLD=100
ITERATIONS=25
INDICES=np.indices((256,256))

class MShift(object):
    #
    # PUBLIC METHODS
    #
    def __init__(self,data,
            width=WIDTH,
            min_count=MIN_COUNT,
            intensity_threshold=INTENSITY_THRESHOLD,
            iterations=ITERATIONS):
        self.data=data
        self.width=width/100.0
        self.min_count=min_count
        self.threshold=intensity_threshold
        self.iterations=iterations
        self._init_properties()


    def input_data(self):
        if self._input_data is None:
            ijimage=np.apply_along_axis(self._ijdata,0,INDICES)
            ij_rows=ijimage.reshape(3,-1)
            zeros=np.all(np.equal(ij_rows, 0),axis=0)
            self._input_data=ij_rows[:,~zeros]
        return self._input_data.astype(int)


    def clustered_data(self):
        if self._clusters is None:   
            self._clusters=np.delete(self.input_data().copy(),2,axis=0).reshape(-1,2)
            for n in range(self.iterations):
                for i, x in enumerate(self._clusters):
                    dist = np.sqrt(((x-self._clusters)**2).sum(1))
                    weight = self._gaussian(dist)
                    self._clusters[i] = (np.expand_dims(weight,1)*self._clusters).sum(0) / weight.sum()
        return self._clusters


    def clusters(self):
        xu,count=np.unique(self.clustered_data(),axis=0,return_counts=True)
        points=[]
        for t,cnt in zip(xu,count): 
            if cnt>=self.min_count: 
                points.append(t.tolist())
        points=np.array(points)
        return points.astype(int)


    #
    # INTERNAL 
    #
    def _init_properties(self):
        self._input_data=None
        self._clusters=None


    def _gaussian(self,d):
        return np.exp(-0.5*((d/self.width))**2) / (self.width*math.sqrt(2*math.pi))


    def _ijdata(self,idx):
        i=idx[0]
        j=idx[1]
        val=self.data[i,j]
        if val>self.threshold:
            return [i,j,val]
        else:
            return np.zeros(3)





