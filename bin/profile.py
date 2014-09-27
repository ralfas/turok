#!/usr/bin/env python

import cProfile
import pstats

import argparse

import sys
from os.path import dirname, realpath

sys.path.append(dirname(dirname(realpath(__file__))))

import apply as apply2

def main():
	cProfile.run('apply2.aggregate([1, 2, 3, 4, 5, 2, 3, 4, 5, 2, 3, 4, 5, 2, 3, 4, 5, 2, 3, 4, 5], [1, 2, 3, 4, 5, 2, 3, 4, 5, 2, 3, 4, 5, 2, 3, 4, 5, 2, 3, 4, 5], "sum")', "profile.prof")
	s = pstats.Stats("profile.prof")
	s.strip_dirs().sort_stats("time").print_stats()

if __name__ == '__main__':
	main()