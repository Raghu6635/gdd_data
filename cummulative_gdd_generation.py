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