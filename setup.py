
from distutils.core import setup
setup(
  name = 'glad_clusters',
  packages = ['glad_clusters'],
  version = '0.0.0.1',
  description = 'MeanShift Clustering of GLAD alerts with AWS-Lambda',
  author = 'Brookie Guzder-Williams',
  author_email = 'bguzder-williams@wri.org',
  url = 'https://github.com/wri/mean_shift_lambda',
  download_url = 'https://github.com/wri/mean_shift_lambda/tarball/0.1',
  keywords = ['Clustering','MeanShift', 'AWS','Lambda','GLAD'],
  include_package_data=True,
  install_requires=['boto3', 'numpy', 'pandas', 'psycopg2', 'scikit-image', 'pyyaml'],
  data_files=[
    (
      'config',[]
    )
  ],
  classifiers = [],
  entry_points={
      'console_scripts': [
          'glad_clusters=glad_clusters.utils.service:main'
      ],
  }
)