import math
import numpy as np
from clusters.convex_hull import ConvexHull
import clusters.processors as proc

NOISY=False
WIDTH=15
MIN_COUNT=6
ITERATIONS=25
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
            iterations=ITERATIONS):
        self.data=data
        self.width=width
        self.min_count=min_count
        self.iterations=iterations
        self._init_properties()


    def centered_data(self):
        """ add indices (centered at origin) to data

            Returns: 
                array of [i,j,days-since] valued arrays
        """
        if self._centered_data is None:
            i=np.subtract(INDICES[0],SHIFT)
            j=np.subtract(INDICES[1],SHIFT)
            self._centered_data=np.dstack((i,j,self.data))
            self._centered_data=self._centered_data.reshape(SIZE**2,-1)
            self._centered_data=self._centered_data[self._centered_data[:,-1]>0]
        return self._centered_data


    def clustered_data(self):
        """ shift alert i,j values using mean-shift
            to final value centroid x,y position

            Returns: 
                array of [x,y] valued arrays
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
            
            * groups points at a given x,y
            * thresholds for nb_pts>min_count

            Returns: 
                array of [x,y,count] valued arrays
        """
        if self._clusters is None:   
            xys,counts=np.unique(
                self.clustered_data(),
                axis=0,
                return_counts=True)
            counts=np.expand_dims(counts,axis=-1)
            self._clusters=np.concatenate(
                (xys,counts),
                axis=-1)
            self._clusters=self._clusters[
                self._clusters[:,-1]>=self.min_count]
        return self._clusters


    def clusters_data(self):
        """ dictionary
        """
        cluster_dict={}
        cluster_dict['input_data']=self.centered_data()
        cluster_dict['nb_clusters']=len(self.clusters())
        cluster_dict['clusters']=[
            self.cluster_data(c) for c in self.clusters()]
        return cluster_dict


    def cluster_data(self,cluster):
        """ dictionary
        """
        x,y,count=cluster
        ijds=self._ijds_for_xy(x,y)
        area=ConvexHull(ijds[:,:-1]).area
        min_date=proc.date_for_days(np.amin(ijds[:,-1]))
        max_date=proc.date_for_days(np.amax(ijds[:,-1]))
        cluster_dict={
            'x':x,
            'y':y,
            'count':count,
            'area':area,
            'max_date':max_date,
            'min_date':min_date,
            'alerts':ijds }
        return cluster_dict


    #
    # INTERNAL 
    #
    def _init_properties(self):
        self._centered_data=None
        self._clustered_data=None
        self._joined=None
        self._clusters=None


    def _joined_data(self):
        if self._joined is None:
            self._joined=np.concatenate(
                (self.clustered_data(),self.centered_data()),axis=-1)
        return self._joined


    def _ijds_for_xy(self,x,y):
        is_in_cluster=np.logical_and(
            self._joined_data()[:,0]==x,
            self._joined_data()[:,1]==y)
        return self._joined_data()[is_in_cluster][:,2:]


    def _gaussian(self,d):
        return np.exp(-0.5*((d/self.width))**2) / (self.width*math.sqrt(2*math.pi))




