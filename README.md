## WIP

This library is under construction, needs testing, etc.

# Go streaming download

This is a HTTP download library that writes a file its downloading to a file while also presenting an 
`io.Writer` so the download can be processed while being downloaded. Downloads are automatically resumed when necessary.

# TODO

- Add tests
- Look at Modified and ETag headers to determine whether we are dealing with the same file
- Check error from doDownloadRequest and only do retries when its a connection error
- ???