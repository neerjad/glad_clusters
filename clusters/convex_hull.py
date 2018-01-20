
class ConvexHull(object):
    #
    # PUBLIC METHODS
    #
    def __init__(self,points):
            self.points=points
            self.hull=self._convex_hull(points)
            self.area=self._area(self.hull)


    """ compute convex hull
        https://medium.com/100-days-of-algorithms/day-28-convex-hull-bc84b678da06
    """
    def _split(self,u, v, points):
        return [p for p in points if np.cross(p - u, v - u) < 0]


    def _extend(self,u, v, points):
        if not points:
            return []
        w = min(points, key=lambda p: np.cross(p - u, v - u))
        p1, p2 = self._split(w, v, points), self._split(u, w, points)
        return self._extend(w, v, p1) + [w] + self._extend(u, w, p2)


    def _convex_hull(self,points):
        u = min(points, key=lambda p: p[0])
        v = max(points, key=lambda p: p[0])
        left, right = self._split(u, v, points), self._split(v, u, points)
        return np.array([v] + self._extend(u, v, left) + [u] + self._extend(v, u, right) + [v])


    """ compute area
        https://stackoverflow.com/a/30408825/607528
    """
    def _area(self,pts_arr):
        x=pts_arr[:,0]
        y=pts_arr[:,1]
        return 0.5*np.abs(np.dot(x,np.roll(y,1))-np.dot(y,np.roll(x,1))) 