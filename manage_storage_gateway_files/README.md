# Copying Files
I attempted 3 basic methods for copying files.

## Mount Points
In order to use mount points, a user must either be on campus, or connected using the VPN.
In testlibnd, these are the storage gateway smb mount points:
* smb://172.22.242.253/testlibnd-smb-test  with a username of smbguest
* smb://172.22.242.253/trbsc-test-files  with a username of smbguest

In libnd, these are the storage gateway smb mount points:
* smb://loadingdock.library.nd.edu/libnd-smb-rbsc
* smb://loadingdock.library.nd.edu/libnd-smb-marble


## Copy using mount points for the storage gateways
Simply connect to whichever mount points are desired, and drag and drop from one window to another.  Using this method, I was able to copy about 20 Gig in about 10 hours.

### Advantages
* This maintains permissions, and datestamps.

### Disadvantages
* This is profoundly slow


## Copy using cp command between mount points
Connect to mount points and execute ```./slow_copy_marble_image_folders_with_cp.sh```
This copied about 6 Gig in about 3 hours.

### Advantages
* This maintains permissions.

### Disadvantages
* Timestamps are reset to now.
* This is profoundly slow.


## Copy using s3 sync
There is no need to connect to mount points, since this uses the aws cli.  This requires 2 steps.
1. Copy files using ```./fast_copy_marble_image_folders_with_s3_sync.sh```
2. refrest the storage gateway cache by running 
```python -c "import refresh_storage_gateway_cache; refresh_storage_gateway_cache.test()"```

### Advantages
* Dramatically faster.  I was able to copy 4 Gig in about 30 seconds because files don't have to be copied from the cloud through my machine and back to the cloud.
* Does not require a connection to the VPN
* Can be resumed after interruption without re-copying content that was already copied

### Disadvantages
* Timestamps are reset to now
* All ACLS are lost.  Basically anyone with access to the Storage Gateway mountpoint will have full access to the files.  (Note:  This isn't a concern for us with this project.)
* Copied files will be invisible to the Storage Gateway until the cache is manually rebuilt (using call noted above).
* At ND, developers do not have permissions to perform: storagegateway:RefreshCache - so we have to rely on someone in ESU to refresh the cache.

