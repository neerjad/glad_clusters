
from distutils.core import setup
setup(
  name = 'glad_cluster_service',
  packages = [],
  version = '0.0.0.1',
  description = 'MeanShift Clustering of GLAD alerts with AWS-Lambda',
  author = 'Brookie Guzder-Williams',
  author_email = 'bguzder-williams@wri.org',
  url = 'https://github.com/wri/mean_shift_lambda',
  download_url = 'https://github.com/wri/mean_shift_lambda/tarball/0.1',
  keywords = ['Clustering','MeanShift', 'AWS','Lambda','GLAD'],
  include_package_data=True,
  data_files=[
    (
      'config',[]
    )
  ],
  classifiers = [],
  entry_points={
      'console_scripts': [
          'glad_clusters=utils.service:main',
      ],
  }
)