
# coding: utf-8

# In[27]:


"""
Main task:   execute_function (Example: gdd_evaluate, smoothing)

wrapper function: myfunction()

main function: command line arguments, parallel processing, post processing

2012, corn:
em,har,gdd -> Rasterstacks
757*1521 pixels

every pixel log:
myfunction(em,har,gdd){
        gdd dataframe construct (365 rows)
        return gdd_evaluate(em_pixel,har_pixel,gdd_dataframe,crop='corn')
}
gdd_evaluate(e,h,gdd_df){
       row1=which(gdd_df('c1'=e))
       row2=which(gdd_df('c1'=h))
       return cummulative_sum(gd,row1,row2)
}

"""


# In[28]:


import pandas as pd
import numpy as np
import rasterio
import time
import random,sys,getopt
from multiprocessing import Pool


# In[4]:
        

def dataframe_extraction(r,row,col):
    data_frame = (pd.DataFrame({'GDD':r[:,row,col]}))
    data_frame['SUM'] = data_frame.GDD.cumsum()
   
    return data_frame




# In[29]:


def initialize_rasters(path1,path2,path3):
    raster1=rasterio.open(path1)
    tot_cols=raster1.width
    tot_rows=raster1.height
    a=raster1.read()
    print("Tot rows: ",tot_rows," Tot cols: ",tot_cols)
    raster2=rasterio.open(path2)
    b=raster2.read()
    raster3=rasterio.open(path3)
    c=raster3.read()
    if(b.shape[0]!=a.shape[0]):
        print("determine what doy you are missing!!")
        t1=b[0:6] #determine what doy you are missing!! in this case its DOY49 thus insert as 7th layer
        t2=b[6:b.shape[0]]
        p=b[0]*0
        p=p.reshape(1,tot_rows,tot_cols)

        tp=np.append(t1,p,axis=0)
        b=np.append(tp,t2,axis=0)
        #print(b.shape)
    return (a,b,c,tot_rows,tot_cols)


# In[30]:


def myfunction(index,rasters):
    a=rasters[0]
    b=rasters[1]
    c=rasters[2]
    tot_cols=a.shape[2]
    tot_rows=a.shape[1]
    #print("Tot rows: ",tot_rows," Tot cols: ",tot_cols)
    row=int(index/tot_cols)
    col=index-(tot_cols*row)
    #print("row: ",row," col: ",col)
    l1 = a[0,row,col]
    l2 = b[0,row,col]
    
    gdd = 0
    if l1 != 65535:
        df=dataframe_extraction(c,row,col)
        gdd=df.SUM[l2-1]-df.SUM[l1-2]

    return gdd


# The doy interval we need to consider for the dataframe passed into ritvik_fn changes from crop to crop and ste to state. For eg, for winter wheat in Kansas, greep up happens in 1:60 doys and sennescence in 125:200 doys.




# In[33]:


"""a,b,c,tot_rows,tot_cols=initialize_rasters('emergence_harvesting_data/data_480/emergence_corn_2012_480.tif','emergence_harvesting_data/data_480/harvesting_corn_2012_480.tif','gdd_data/gdd_corn_2012.tif')

print(a.shape)
print(b.shape)
print(c.shape)
print(tot_rows, tot_cols)

row=343
col=1000
l1 = a[:,row,col]
l2 = b[:,row,col]
print(l1)
print(l2)
gdd = 0
if l1 != 65535:
    df=dataframe_extraction(c,row,col)
    gdd=df.SUM[l2-1]-df.SUM[l1-2]
print(gdd)"""


# In[32]:


# In[8]:

#a,b,tot_rows,tot_cols=initialize_rasters("A2015_177_ndvi_480m.tif","A2015_185_ndvi_480m.tif")
def main(argv):
    opts,args=getopt.getopt(argv,"hp:m:r:s:",["processes=","mode=","run=","side="])
    for opt,arg in opts:
        if opt=="-h":
            print("python crop_phenology.py -p 16 -m 100 -r s -s 1 ")
            print("Give the number of processes into -p switch and the number of pixels to run as -m switch and the way to run as -r switch and which half of the raster as --side if -s 0 is given then the total raster is evaluated")
            print("python crop_phenology.py -p 16 -m 0 -r s|p  -s 0|1|2   -> m=0 implies run on all pixels and r=s implies run sequentially where as r=p implies running paralelly")
            sys.exit()
        elif opt in ("-p","--processes"):
            num_proc=int(arg)
        elif opt in ("-m","--mode"):
            index_input=int(arg)
            if index_input==0:
                ind_start=0
            else:
                ind_start=988650+500
        elif opt in ("-r","--run"):
            run=arg
        elif opt in ("-s","--side"):
            side=int(arg)    

    src=rasterio.open('emergence_harvesting_data/data_480/emergence_corn_2012_480.tif')
    a,b,c,tot_rows,tot_cols=initialize_rasters('emergence_harvesting_data/data_480/emergence_corn_2012_480.tif','emergence_harvesting_data/data_480/harvesting_corn_2012_480.tif','gdd_data/gdd_corn_2012.tif')
    y=[(a,b,c)]*tot_rows*tot_cols

    ind=range(tot_rows*tot_cols)
    l=list()
    l=list(list(zip(ind,y))[:])
    #greenup=np.zeros(shape=a[1].shape)
    #sen=greenup

    start=time.time()
    if index_input==0:
            if(side==1):
                ind_start=0
                ind_end=int(len(l)/2)
            elif(side==2):
                ind_start=int(len(l)/2)
                ind_end=len(l)
            elif(side==0):
                ind_start=0
                ind_end=len(l)
            #print(ind_end)
    else:
            ind_end=ind_start+index_input

    print("ind_start: ",ind_start," ind_end: ",ind_end)
    if(run=="p"):
        with Pool(processes=num_proc) as pool:
            #ind_start=988800
            #ind_end=ind_start+1000
            values=pool.starmap(myfunction,l[ind_start:ind_end])
            pool.close()
            pool.join()
    elif(run=="s"):
        for i in range(ind_start,ind_end):
            values.append(myfunction(l[i][0],l[i][1]))
    end=time.time()
    print(end-start)
    #print(pairs)


    profile=src.profile
    profile.update(count=1)
    print(profile)
    gdd=np.zeros(shape=a.shape)
    for index in list(range(ind_start,ind_end)):
        row=int(index/tot_cols)
        col=index-(tot_cols*row)
        gdd[0][row][col]=values[index-ind_start]
           
        
    with rasterio.open('cummulative_gdd_output/test/gdd_emergence_harvest.tif', 'w', **profile) as dst:
            dst.write(gdd.astype(rasterio.uint16), 1)
    


if __name__=="__main__":
    main(sys.argv[1:])


# In[ ]:




