import math
import numpy as np


NOISY=False
WIDTH=15
MIN_COUNT=6
ITERATIONS=25
DOWNSAMPLE=False
SIZE=256
INDICES=np.indices((SIZE,SIZE))

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
        self.width=width
        self.min_count=min_count
        self.iterations=iterations
        self.downsample=downsample
        self._init_properties()


    def input_data(self):
        if self._input_data is None:
            ix=np.subtract(INDICES[0],(SIZE-1)/2.0)
            iy=np.subtract(INDICES[1],(SIZE-1)/2.0)
            self._input_data=np.dstack((ix,iy,self.data))
            self._input_data=self._input_data.reshape(SIZE**2,-1)
            self._input_data=self._input_data[self._input_data[:,-1]>0]
        return self._input_data


    def clustered_data(self):
        if self._clusters is None:   
            values=self.input_data()[:,-1]
            self._clusters=np.delete(self.input_data().copy(),-1,axis=1)
            for n in range(self.iterations):
                if NOISY: 
                    if (n+1)%5==0: print("...{}/{}".format(n+1,self.iterations))
                for i, x in enumerate(self._clusters):
                    dist = np.sqrt(((x-self._clusters)**2).sum(1))
                    weight = self._gaussian(dist)
                    self._clusters[i] = (np.expand_dims(weight,1)*self._clusters).sum(0) / weight.sum()
        return self._clusters


    def clusters(self):
        points=[]
        if len(self.clustered_data()):
            if self.downsample:
                # TODO: THINK MORE ABOUT THIS STEP & UPSAMPLING
                cluster_data=self.clustered_data()/self.downsample
            else:
                cluster_data=self.clustered_data()
            xu,count=np.unique(cluster_data.astype(int),axis=0,return_counts=True)
            for t,cnt in zip(xu,count): 
                if cnt>=self.min_count: 
                    t=np.append(t,cnt)
                    points.append(t.tolist())
        return np.array(points)


    #
    # INTERNAL 
    #
    def _init_properties(self):
        self._input_data=None
        self._clusters=None


    def _gaussian(self,d):
        return np.exp(-0.5*((d/self.width))**2) / (self.width*math.sqrt(2*math.pi))




