import math
import numpy as np


NOISY=False
WIDTH=15
MIN_COUNT=6
ITERATIONS=25
DOWNSAMPLE=False
SIZE=256
INDICES=np.indices((SIZE,SIZE))
SHIFT=(SIZE-1)/2.0

class MShift(object):


    @staticmethod
    def zero_shifted_list(data_arr):
        data_arr=data_arr.copy()
        data_arr[:,0]=np.add(data_arr[:,0],SHIFT)
        data_arr[:,1]=np.add(data_arr[:,1],SHIFT)
        return data_arr.astype(int).tolist()



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


    def centered_data(self):
        if self._centered_data is None:
            ix=np.subtract(INDICES[0],SHIFT)
            iy=np.subtract(INDICES[1],SHIFT)
            self._centered_data=np.dstack((ix,iy,self.data))
            print(self._centered_data)
            self._centered_data=self._centered_data.reshape(SIZE**2,-1)
            self._centered_data=self._centered_data[self._centered_data[:,-1]>0]
        return self._centered_data


    def clustered_data(self):
        """ shift i,j values using meanshift algo
            
            * groups points at a given i,j
            * thresholds for nb_pts>min_count

            Returns: 
                array of [i,j] valued arrays
        """
        if self._clustered_data is None:   
            self._clustered_data=self.centered_data()[:,:2].copy()
            for n in range(self.iterations):
                if NOISY: 
                    if (n+1)%5==0: print("...{}/{}".format(n+1,self.iterations))
                for i, x in enumerate(self._clustered_data):
                    dist=np.sqrt(((x-self._clustered_data)**2).sum(1))
                    weight=self._gaussian(dist)
                    self._clustered_data[i]=(
                        np.expand_dims(weight,1)*self._clustered_data).sum(0)/weight.sum()
                self._clustered_data=self._clustered_data.round().astype(int)
        return self._clustered_data


    def clusters(self):
        """ group into clusters
            
            * groups points at a given i,j
            * thresholds for nb_pts>min_count

            Returns: 
                array of [i,j] valued arrays
        """
        if self._clusters is None:   
            ijs,count=np.unique(
                self.clustered_data(),
                axis=0,
                return_counts=True)
            self._clusters=np.concatenate(
                (ijs,np.expand_dims(count,axis=-1)),
                axis=-1)
            self._clusters=self._clusters[
                self._clusters[:,-1]>self.min_count]
        return self._clusters


    #
    # INTERNAL 
    #
    def _init_properties(self):
        self._centered_data=None
        self._clustered_data=None
        self._clusters=None


    def _gaussian(self,d):
        return np.exp(-0.5*((d/self.width))**2) / (self.width*math.sqrt(2*math.pi))




