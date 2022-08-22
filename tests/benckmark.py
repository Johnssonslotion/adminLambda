import time



def TEST_NORMALSCAN(conn):
    st=time.time()
    r=conn.client.scan(
        TableName="build_info",
        IndexName="geohash-index",
        Limit=1000
    )
    
    print(f"Full indexing : Time {time.time()-st}")
    st=time.time()
    r=conn.client.scan(
        TableName="build_info",
        IndexName="geohash-search",
        Limit=1000
    )
    
    print(f"Short indexing : Time {time.time()-st}")



## benchmark for geohash opti by grid search
def TEST_HASHOPT(conn):
    default_cord={
        'lat':36.4977,
        'lng':127.2067,
        'radius':1000 
    }
    
    for i in range(1,5,1):
        radius=pow(10,i)
        st=time.time()
        #def radius_query(self,Lat,Lng,radius,Table=None,Index=None):
        conn.query_radius(Lat=default_cord['lat'],Lng=default_cord['lng'],radius=radius,Table='TEST_CASE_0_build_info',Index=None,Hash=5)
        print(f"TEST_CASE_0,HASH : 5,\t\t\t Full col  \t\t\t r: {radius}  \t\t\t Time {time.time()-st:.3f}")
        st=time.time()
        conn.query_radius(Lat=default_cord['lat'],Lng=default_cord['lng'],radius=radius,Table='TEST_CASE_0_build_info',Index='geohash-opt',Hash=5)
        print(f"TEST_CASE_0,HASH : 5,\t\t\t short Col \t\t\t r: {radius}  \t\t\t Time {time.time()-st:.3f}")
    
    for i in range(1,5,1):
        radius=pow(10,i)
        st=time.time()
        conn.query_radius(Lat=default_cord['lat'],Lng=default_cord['lng'],radius=radius,Table='TEST_CASE_1_build_info',Index=None,Hash=7)
        print(f"TEST_CASE_1,HASH : 7,\t\t\t Full col  \t\t\t r: {radius}  \t\t\t Time :{time.time()-st:.3f}")
        st=time.time()
        conn.query_radius(Lat=default_cord['lat'],Lng=default_cord['lng'],radius=radius,Table='TEST_CASE_1_build_info',Index='geohash-opt',Hash=7)
        print(f"TEST_CASE_1,HASH : 7,\t\t\t short Col  \t\t\t r: {radius} \t\t\t Time :{time.time()-st:.3f}")
        
        
    for i in range(1,5,1):
        radius=pow(10,i)
        st=time.time()
        conn.query_radius(Lat=default_cord['lat'],Lng=default_cord['lng'],radius=radius,Table='TEST_CASE_2_build_info',Hash=9)
        print(f"TEST_CASE_2,HASH : 9,\t\t\t Full col \t\t\t r: {radius} \t\t\t Time :{time.time()-st:.3f}")
        st=time.time()
        conn.query_radius(Lat=default_cord['lat'],Lng=default_cord['lng'],radius=radius,Table='TEST_CASE_2_build_info',Index='geohash-opt',Hash=9)
        print(f"TEST_CASE_2,HASH : 9,\t\t\t short Col \t\t\t r: {radius} \t\t\t Time :{time.time()-st:.3f}")
    
    