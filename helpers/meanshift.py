import math
import numpy as np

DEFAULT_WIDTH=0.15
DEFAULT_ITERATIONS=25
INDICES=np.indices((256,256))
THRESH=100
MIN_COUNT=4

class MShift(object):


    def __init__(self,data,width=DEFAULT_WIDTH):
        self.data=data
        self.width=width


    def clusters(self,iterations=DEFAULT_ITERATIONS):
        self._clusters=np.delete(self.shiftdata(),2,axis=0).reshape(-1,2)
        for n in range(iterations):
            for i, x in enumerate(self._clusters):
                dist = np.sqrt(((x-self._clusters)**2).sum(1))
                weight = self._gaussian(dist, self.width)
                self._clusters[i] = (np.expand_dims(weight,1)*self._clusters).sum(0) / weight.sum()
        return self._clusters


    def unq(self,data):
        xu,count=np.unique(data,axis=0,return_counts=True)
        points=[]
        for t,cnt in zip(xu,count): 
            if cnt>=MIN_COUNT: 
                points.append(t.tolist())
        points=np.array(points)
        return points.astype(int)


    def shiftdata(self):
        ijimage=np.apply_along_axis(self._ijdata,0,INDICES)
        ij_rows=ijimage.reshape(3,-1)
        zeros=np.all(np.equal(ij_rows, 0),axis=0)
        ijvals=ij_rows[:,~zeros]
        return ijvals


    def _gaussian(self,d, bw):
        return np.exp(-0.5*((d/bw))**2) / (bw*math.sqrt(2*math.pi))


    def _ijdata(self,idx):
        i=idx[0]
        j=idx[1]
        val=self.data[i,j]
        if val>THRESH:
            return [i,j,val]
        else:
            return np.zeros(3)





