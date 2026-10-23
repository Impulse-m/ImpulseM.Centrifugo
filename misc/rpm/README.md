RPM for Centrifugo

Note: the fork release flow does not use this directory. The fork publishes Linux tar.gz archives and Docker images, see `.github/workflows/images.yml`; the steps below are upstream's and are kept as they are.

```
yum -y install rpmdevtools
```

Go to directory containing all files listed here.

Download release to current folder:

```
wget https://github.com/centrifugal/centrifugo/releases/download/v0.0.1/centrifugo-0.0.1-linux-amd64.zip -O centrifugo-0.0.1-linux-amd64.zip
```

Build rpm:

```
./build.sh 0.0.1
```

As argument to `build.sh` you should specify Centrifugo version of release binary downloaded above.