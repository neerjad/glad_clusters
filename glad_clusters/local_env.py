from __future__ import print_function
import yaml
import os

DIR=os.path.dirname(os.path.abspath(__file__))
NOISY=True

with open("{}/{}".format(DIR,'env.yml')) as file:
    env = yaml.safe_load(file)


def export(env_name='dev'):
    if NOISY: print("\nEXPORTING {} ENV:".format(env_name))
    for k,v in env.get(env_name).iteritems():
        if NOISY:
            print("\t{}: {}".format(k,v))
            os.environ[k]=v
    if NOISY: print("\n")


#
#   MAIN
#
if __name__ == "__main__":
    import argparse
    parser=argparse.ArgumentParser(description='EXPORT ENV')
    parser.add_argument('env',help='environment to export')
    args=parser.parse_args()
    export(args.env)