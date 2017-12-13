import imageio as io


class GLAD(object):
    #
    # PUBLIC METHODS
    #
    def __init__(self,data_path):
        self.data_path=data_path
        self.raw_data=io.imread(data_path)


    def data(self,start_date=None,end_date=None):
        self.start_date=start_date
        self.end_date=end_date
        return self.raw_data
