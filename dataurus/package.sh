#!/bin/bash

tar --dereference --exclude='build' --exclude='submission.tar.gz' --exclude='workloads' -czf submission.tar.gz *
