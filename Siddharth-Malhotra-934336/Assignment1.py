# File     : Assignment1.py
# Author   : Siddharth Malhotra
# Purpose  : Counting the number of Instagram posts  as per coordinates location

import re
import os
import time
import json

from mpi4py import MPI

#Setting the size and rank for MPI
size = MPI.COMM_WORLD.Get_size()
rank = MPI.COMM_WORLD.Get_rank()

#Method for MPI pationing and allocation of resources.
def computation(linecount,partition):
    melb_Grid = {}
    block_result = {}
    coordinates = []
    searchcoord = []

     #Each core gets the respected begin and complete variable for processing
    if rank != size-1:
        begin = int(partition * rank)
        complete = int(partition * rank + partition)
    else:
        begin = int(partition * rank)
        complete = linecount

    #Using regular expression for detecting the coordinates and appending them to a list for future reference
    with open('bigInstagram.json') as file:
        for i in range(0, begin):
            file.readline()
        for i in range(begin, complete):
            searchcoord = re.search(',"coordinates":{"type":"Point","coordinates":\[(.*?)\]}', file.readline())
            if searchcoord:
                semi = searchcoord.group(1).split(",")
                if semi[0] == 'null' and semi[1] == 'null':
                    continue
                else:
                    coordinates.append([float(semi[0]), float(semi[1])])

    #Storing Melbourne grid data values and initializing thme to 0 under the block_results dictionary 
    loadmelbgrid = json.loads(open('melbGrid.json', 'r').read())
    for block in loadmelbgrid['features']:
        melb_Grid[block['properties']['id']] = (block['properties']['xmin'], 
            block['properties']['xmax'], block['properties']['ymin'], block['properties']['ymax']) 
        block_result[block['properties']['id']] = 0

    #Incrementing value of coordinates by one each time, thus letting us know the available number of tweets per location
    for point in coordinates:
        for key, value in melb_Grid.items():
            if value[0] < point[1] <= value[1]:
                if value[2] < point[0] <= value[3]:
                    block_result[key]+= 1

    final_result = MPI.COMM_WORLD.gather(block_result, root=0)

    return final_result

#Generic function to count the number of lines in the Instagram file, sub-parts which is later utilized for allocation 
#for different cores 
def countlines():
    with open('bigInstagram.json', 'r') as g:
        for line in g: 
            linecount = -2  
            while line:
                linecount += 1
                line = g.readline()
            g.close()
            return linecount

#Displaying the coordinates as per the blocks, rows and columns
def finalfunc(final_result):
    if rank == 0:
        sum_row = {}
        sum_column = {}
        sum_block = {}
        for i in range(0, size):
            for key, value in final_result[i].items():
                if key not in sum_block:
                    try: 
                        sum_block[key] = 0
                    except Exception:
                        return None
                sum_block[key] = sum_block[key] + value
                if key not in sum_row:
                    try: 
                        sum_row.setdefault(key[0], 0)
                    except Exception:
                        return None
                sum_row[key[0]] = sum_row[key[0]] + value
                if key not in sum_column:
                    try: 
                        sum_column.setdefault(key[1:], 0)
                    except Exception:
                        return None
                sum_column[key[1:]] = sum_column[key[1:]] + value

        blocks = sorted([(key, value) for key, value in sum_block.items()], key=lambda x:x[1])
        blocks.reverse()
        print('In block')
        for item in blocks:
            print('%s: %d posts' % (item[0], item[1]))

        row = sorted([(key, value) for key, value in sum_row.items()], key=lambda x:x[1])
        row.reverse()
        print('In row')
        for row in row:
            print('%s: %d posts' % (row[0], row[1]))

        column = sorted([(key, value) for key, value in sum_column.items()], key=lambda x:x[1])
        column.reverse()
        print('In column')
        for col in column:
            print('%s: %d posts' % (col[0], col[1]))

        print('Time: %f ' % (time.clock() - begin))

#Execution begins from here

def main():
    linecount = countlines() 
    partition = linecount / size
    finalfunc(computation(linecount,partition))


if __name__ == "__main__":
    begin = time.clock()
    main()
