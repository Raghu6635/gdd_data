def gdd_evaluate(l1,l2,temp_data,crop):
    years=list(map(str, list(range(2001,2016))))
    gdd=list()
    gdds=list()
    precip=list()
    for i in range(len(years)):
            yr=years[i]
            start_doy=l1[i]
            stop_doy=l2[i]
            t=temp_data[temp_data.Year.amin==yr]
            if(crop[i]==1):
                g=t.gdd_corn[start_doy-1:stop_doy].sum()
                gs=t.gdds_corn[start_doy-1:stop_doy].sum()
            else:
                g=t.gdd_soy[start_doy-1:stop_doy].sum()
                gs=t.gdds_soy[start_doy-1:stop_doy].sum()
            p=t.P[start_doy-1:stop_doy].sum()
            precip.append(p[0])
            gdd.append(g)
            gdds.append(gs)
            print(gs)
    print()
    return (gdd,precip)