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


    def ij_data(self):
        """ add indices to data

            Returns: 
                array of [i,j,days-since] valued arrays
        """
        self._ij_data=np.dstack((INDICES[0],INDICES[1],self.data))
        self._ij_data=self._ij_data.reshape(SIZE**2,-1)
        self._ij_data=self._ij_data[self._ij_data[:,-1]>0]
        return self._ij_data


    def clustered_data(self):
        """ shift alert i,j values using mean-shift
            to final value centroid x,y position.
            then shift back and round to get ij coords

            Returns: 
                array of [i,j] valued arrays
        """
        if self._clustered_data is None:   
            cdata=self.ij_data()[:,:2].copy()
            cdata=np.subtract(cdata,SHIFT)
            for n in range(self.iterations):
                if NOISY: 
                    if (n+1)%5==0: print("...{}/{}".format(n+1,self.iterations))
                for i, x in enumerate(cdata):
                    dist=np.sqrt(((x-cdata)**2).sum(1))
                    weight=self._gaussian(dist)
                    cdata[i]=(
                        np.expand_dims(weight,1)*cdata).sum(0)/weight.sum()
            self._clustered_data=np.add(cdata,SHIFT).round().astype(int)
        return self._clustered_data


    def clusters(self):
        """ group into clusters
            
            * groups points at a given i,j
            * thresholds for nb_pts>min_count

            Returns: 
                array of [i,j,count] valued arrays
        """
        if self._clusters is None:   
            points,counts=np.unique(
                self.clustered_data(),
                axis=0,
                return_counts=True)
            counts=np.expand_dims(counts,axis=-1)
            self._clusters=np.concatenate(
                (points,counts),
                axis=-1)
            self._clusters=self._clusters[
                self._clusters[:,-1]>=self.min_count]
        return self._clusters


    def clusters_data(self):
        """ dictionary
        """
        cluster_dict={}
        cluster_dict['input_data']=self.ij_data().astype(int).tolist()
        cluster_dict['nb_clusters']=len(self.clusters())
        cluster_dict['clusters']=[
            self.cluster_data(c) for c in self.clusters()]
        return cluster_dict


    def cluster_data(self,cluster):
        """ dictionary
        """
        i,j,count=cluster
        alerts=self._alerts_for_points(i,j)
        area=ConvexHull(alerts[:,:-1]).area
        min_date=proc.date_for_days(np.amin(alerts[:,-1]))
        max_date=proc.date_for_days(np.amax(alerts[:,-1]))
        cluster_dict={
            'i':i,
            'j':j,
            'count':count,
            'area':int(round(area)),
            'max_date':max_date,
            'min_date':min_date,
            'alerts':alerts.astype(int).tolist() }
        return cluster_dict


    #
    # INTERNAL 
    #
    def _init_properties(self):
        self._ij_data=None
        self._clustered_data=None
        self._joined=None
        self._clusters=None


    def _joined_data(self):
        if self._joined is None:
            self._joined=np.concatenate(
                (self.clustered_data(),self.ij_data()),axis=-1)
        return self._joined


    def _alerts_for_points(self,i,j):
        is_in_cluster=np.logical_and(
            self._joined_data()[:,0]==i,
            self._joined_data()[:,1]==j)
        alerts=self._joined_data()[is_in_cluster][:,2:]
        return alerts


    def _gaussian(self,d):
        return np.exp(-0.5*((d/self.width))**2) / (self.width*math.sqrt(2*math.pi))




