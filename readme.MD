# Go DB Sync Configuration

## Environment Variables

Ensure you have set up the following environment variables for the proper functioning of the `go_db_sync` application:

### Database 1 Configuration:

DBMain
```env
DB_USERNAME1=un1
DB_PASSWORD1=pwd1
DB_HOST1=host1
DB_NAME1=dname1
DB_SSL1=
```

DB2
```env
DB_USERNAME2=ok
DB_PASSWORD2=ok
DB_HOST2=100.0.0.1
DB_NAME2=dbname2
DB_SSL2=
```

Only add if you want to dump tbl
```env
COLS=id,timestamp,site_id,speed 
TBL=vehicle
```