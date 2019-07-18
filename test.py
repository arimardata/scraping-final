class GridFSBucket(object):
    """An instance of GridFS on top of a single Database."""
 
    def __init__(self, db, bucket_name="fs",
                 chunk_size_bytes=DEFAULT_CHUNK_SIZE, write_concern=None,
                 read_preference=None):
        """Create a new instance of :class:`GridFSBucket`.
 
        Raises :exc:`TypeError` if `database` is not an instance of
        :class:`~pymongo.database.Database`.
 
        Raises :exc:`~pymongo.errors.ConfigurationError` if `write_concern`
        is not acknowledged.
 
        :Parameters:
          - `database`: database to use.
          - `bucket_name` (optional): The name of the bucket. Defaults to 'fs'.
          - `chunk_size_bytes` (optional): The chunk size in bytes. Defaults
            to 255KB.
          - `write_concern` (optional): The
            :class:`~pymongo.write_concern.WriteConcern` to use. If ``None``
            (the default) db.write_concern is used.
          - `read_preference` (optional): The read preference to use. If
            ``None`` (the default) db.read_preference is used.
 
        .. versionadded:: 3.1
 
        .. mongodoc:: gridfs
        """
        if not isinstance(db, Database):
            raise TypeError("database must be an instance of Database")
 
        wtc = write_concern if write_concern is not None else db.write_concern
        if not wtc.acknowledged:
            raise ConfigurationError('write concern must be acknowledged')
 
        self._db = db
        self._bucket_name = bucket_name
        self._collection = db[bucket_name]
 
        self._chunks = self._collection.chunks.with_options(
            write_concern=write_concern,
            read_preference=read_preference)
 
        self._files = self._collection.files.with_options(
            write_concern=write_concern,
            read_preference=read_preference)
 
        self._chunk_size_bytes = chunk_size_bytes
 
    def open_upload_stream(self, filename, chunk_size_bytes=None,
                           metadata=None):
        """Opens a Stream that the application can write the contents of the
        file to.
 
        The user must specify the filename, and can choose to add any
        additional information in the metadata field of the file document or
        modify the chunk size.
        For example::
 
          my_db = MongoClient().test
          fs = GridFSBucket(my_db)
          grid_in, file_id = fs.open_upload_stream(
                "test_file", chunk_size_bytes=4,
                metadata={"contentType": "text/plain"})
          grid_in.write("data I want to store!")
          grid_in.close()  # uploaded on close
 
        Returns an instance of :class:`~gridfs.grid_file.GridIn`.
 
        Raises :exc:`~gridfs.errors.NoFile` if no such version of
        that file exists.
        Raises :exc:`~ValueError` if `filename` is not a string.
 
        :Parameters:
          - `filename`: The name of the file to upload.
          - `chunk_size_bytes` (options): The number of bytes per chunk of this
            file. Defaults to the chunk_size_bytes in :class:`GridFSBucket`.
          - `metadata` (optional): User data for the 'metadata' field of the
            files collection document. If not provided the metadata field will
            be omitted from the files collection document.
        """
        validate_string("filename", filename)
 
        opts = {"filename": filename,
                "chunk_size": (chunk_size_bytes if chunk_size_bytes
                               is not None else self._chunk_size_bytes)}
        if metadata is not None:
            opts["metadata"] = metadata
 
        return GridIn(self._collection, **opts)
 
    def open_upload_stream_with_id(
            self, file_id, filename, chunk_size_bytes=None, metadata=None):
        """Opens a Stream that the application can write the contents of the
        file to.
 
        The user must specify the file id and filename, and can choose to add
        any additional information in the metadata field of the file document
        or modify the chunk size.
        For example::
 
          my_db = MongoClient().test
          fs = GridFSBucket(my_db)
          grid_in, file_id = fs.open_upload_stream(
                ObjectId(),
                "test_file",
                chunk_size_bytes=4,
                metadata={"contentType": "text/plain"})
          grid_in.write("data I want to store!")
          grid_in.close()  # uploaded on close
 
        Returns an instance of :class:`~gridfs.grid_file.GridIn`.
 
        Raises :exc:`~gridfs.errors.NoFile` if no such version of
        that file exists.
        Raises :exc:`~ValueError` if `filename` is not a string.
 
        :Parameters:
          - `file_id`: The id to use for this file. The id must not have
            already been used for another file.
          - `filename`: The name of the file to upload.
          - `chunk_size_bytes` (options): The number of bytes per chunk of this
            file. Defaults to the chunk_size_bytes in :class:`GridFSBucket`.
          - `metadata` (optional): User data for the 'metadata' field of the
            files collection document. If not provided the metadata field will
            be omitted from the files collection document.
        """
        validate_string("filename", filename)
 
        opts = {"_id": file_id,
                "filename": filename,
                "chunk_size": (chunk_size_bytes if chunk_size_bytes
                               is not None else self._chunk_size_bytes)}
        if metadata is not None:
            opts["metadata"] = metadata
 
        return GridIn(self._collection, **opts)
 
    def upload_from_stream(self, filename, source, chunk_size_bytes=None,
                           metadata=None):
        """Uploads a user file to a GridFS bucket.
 
        Reads the contents of the user file from `source` and uploads
        it to the file `filename`. Source can be a string or file-like object.
        For example::
 
          my_db = MongoClient().test
          fs = GridFSBucket(my_db)
          file_id = fs.upload_from_stream(
              "test_file",
              "data I want to store!",
              chunk_size_bytes=4,
              metadata={"contentType": "text/plain"})
 
        Returns the _id of the uploaded file.
 
        Raises :exc:`~gridfs.errors.NoFile` if no such version of
        that file exists.
        Raises :exc:`~ValueError` if `filename` is not a string.
 
        :Parameters:
          - `filename`: The name of the file to upload.
          - `source`: The source stream of the content to be uploaded. Must be
            a file-like object that implements :meth:`read` or a string.
          - `chunk_size_bytes` (options): The number of bytes per chunk of this
            file. Defaults to the chunk_size_bytes of :class:`GridFSBucket`.
          - `metadata` (optional): User data for the 'metadata' field of the
            files collection document. If not provided the metadata field will
            be omitted from the files collection document.
        """
        with self.open_upload_stream(
                filename, chunk_size_bytes, metadata) as gin:
            gin.write(source)
 
        return gin._id
 
    def upload_from_stream_with_id(self, file_id, filename, source,
                                   chunk_size_bytes=None, metadata=None):
        """Uploads a user file to a GridFS bucket with a custom file id.
 
        Reads the contents of the user file from `source` and uploads
        it to the file `filename`. Source can be a string or file-like object.
        For example::
 
          my_db = MongoClient().test
          fs = GridFSBucket(my_db)
          file_id = fs.upload_from_stream(
              ObjectId(),
              "test_file",
              "data I want to store!",
              chunk_size_bytes=4,
              metadata={"contentType": "text/plain"})
 
        Raises :exc:`~gridfs.errors.NoFile` if no such version of
        that file exists.
        Raises :exc:`~ValueError` if `filename` is not a string.
 
        :Parameters:
          - `file_id`: The id to use for this file. The id must not have
            already been used for another file.
          - `filename`: The name of the file to upload.
          - `source`: The source stream of the content to be uploaded. Must be
            a file-like object that implements :meth:`read` or a string.
          - `chunk_size_bytes` (options): The number of bytes per chunk of this
            file. Defaults to the chunk_size_bytes of :class:`GridFSBucket`.
          - `metadata` (optional): User data for the 'metadata' field of the
            files collection document. If not provided the metadata field will
            be omitted from the files collection document.
        """
        with self.open_upload_stream_with_id(
                file_id, filename, chunk_size_bytes, metadata) as gin:
            gin.write(source)
 
    def open_download_stream(self, file_id):
        """Opens a Stream from which the application can read the contents of
        the stored file specified by file_id.
 
        For example::
 
          my_db = MongoClient().test
          fs = GridFSBucket(my_db)
          # get _id of file to read.
          file_id = fs.upload_from_stream("test_file", "data I want to store!")
          grid_out = fs.open_download_stream(file_id)
          contents = grid_out.read()
 
        Returns an instance of :class:`~gridfs.grid_file.GridOut`.
 
        Raises :exc:`~gridfs.errors.NoFile` if no file with file_id exists.
 
        :Parameters:
          - `file_id`: The _id of the file to be downloaded.
        """
        gout = GridOut(self._collection, file_id)
 
        # Raise NoFile now, instead of on first attribute access.
        gout._ensure_file()
        return gout
 
    def download_to_stream(self, file_id, destination):
        """Downloads the contents of the stored file specified by file_id and
        writes the contents to `destination`.
 
        For example::
 
          my_db = MongoClient().test
          fs = GridFSBucket(my_db)
          # Get _id of file to read
          file_id = fs.upload_from_stream("test_file", "data I want to store!")
          # Get file to write to
          file = open('myfile','wb+')
          fs.download_to_stream(file_id, file)
          file.seek(0)
          contents = file.read()
 
        Raises :exc:`~gridfs.errors.NoFile` if no file with file_id exists.
 
        :Parameters:
          - `file_id`: The _id of the file to be downloaded.
          - `destination`: a file-like object implementing :meth:`write`.
        """
        gout = self.open_download_stream(file_id)
        for chunk in gout:
            destination.write(chunk)
 
    def delete(self, file_id):
        """Given an file_id, delete this stored file's files collection document
        and associated chunks from a GridFS bucket.
 
        For example::
 
          my_db = MongoClient().test
          fs = GridFSBucket(my_db)
          # Get _id of file to delete
          file_id = fs.upload_from_stream("test_file", "data I want to store!")
          fs.delete(file_id)
 
        Raises :exc:`~gridfs.errors.NoFile` if no file with file_id exists.
 
        :Parameters:
          - `file_id`: The _id of the file to be deleted.
        """
        res = self._files.delete_one({"_id": file_id})
        self._chunks.delete_many({"files_id": file_id})
        if not res.deleted_count:
            raise NoFile(
                "no file could be deleted because none matched %s" % file_id)
 
    def find(self, *args, **kwargs):
        """Find and return the files collection documents that match ``filter``
 
        Returns a cursor that iterates across files matching
        arbitrary queries on the files collection. Can be combined
        with other modifiers for additional control.
 
        For example::
 
          for grid_data in fs.find({"filename": "lisa.txt"},
                                  no_cursor_timeout=True):
              data = grid_data.read()
 
        would iterate through all versions of "lisa.txt" stored in GridFS.
        Note that setting no_cursor_timeout to True may be important to
        prevent the cursor from timing out during long multi-file processing
        work.
 
        As another example, the call::
 
          most_recent_three = fs.find().sort("uploadDate", -1).limit(3)
 
        would return a cursor to the three most recently uploaded files
        in GridFS.
 
        Follows a similar interface to
        :meth:`~pymongo.collection.Collection.find`
        in :class:`~pymongo.collection.Collection`.
 
        :Parameters:
          - `filter`: Search query.
          - `batch_size` (optional): The number of documents to return per
            batch.
          - `limit` (optional): The maximum number of documents to return.
          - `no_cursor_timeout` (optional): The server normally times out idle
            cursors after an inactivity period (10 minutes) to prevent excess
            memory use. Set this option to True prevent that.
          - `skip` (optional): The number of documents to skip before
            returning.
          - `sort` (optional): The order by which to sort results. Defaults to
            None.
        """
        return GridOutCursor(self._collection, *args, **kwargs)
 
    def open_download_stream_by_name(self, filename, revision=-1):
        """Opens a Stream from which the application can read the contents of
        `filename` and optional `revision`.
 
        For example::
 
          my_db = MongoClient().test
          fs = GridFSBucket(my_db)
          grid_out = fs.open_download_stream_by_name("test_file")
          contents = grid_out.read()
 
        Returns an instance of :class:`~gridfs.grid_file.GridOut`.
 
        Raises :exc:`~gridfs.errors.NoFile` if no such version of
        that file exists.
 
        Raises :exc:`~ValueError` filename is not a string.
 
        :Parameters:
          - `filename`: The name of the file to read from.
          - `revision` (optional): Which revision (documents with the same
            filename and different uploadDate) of the file to retrieve.
            Defaults to -1 (the most recent revision).
 
        :Note: Revision numbers are defined as follows:
 
          - 0 = the original stored file
          - 1 = the first revision
          - 2 = the second revision
          - etc...
          - -2 = the second most recent revision
          - -1 = the most recent revision
        """
        validate_string("filename", filename)
 
        query = {"filename": filename}
 
        cursor = self._files.find(query)
        if revision < 0:
            skip = abs(revision) - 1
            cursor.limit(-1).skip(skip).sort("uploadDate", DESCENDING)
        else:
            cursor.limit(-1).skip(revision).sort("uploadDate", ASCENDING)
        try:
            grid_file = next(cursor)
            return GridOut(self._collection, file_document=grid_file)
        except StopIteration:
            raise NoFile(
                "no version %d for filename %r" % (revision, filename))
 
    def download_to_stream_by_name(self, filename, destination, revision=-1):
        """Write the contents of `filename` (with optional `revision`) to
        `destination`.
 
        For example::
 
          my_db = MongoClient().test
          fs = GridFSBucket(my_db)
          # Get file to write to
          file = open('myfile','wb')
          fs.download_to_stream_by_name("test_file", file)
 
        Raises :exc:`~gridfs.errors.NoFile` if no such version of
        that file exists.
 
        Raises :exc:`~ValueError` if `filename` is not a string.
 
        :Parameters:
          - `filename`: The name of the file to read from.
          - `destination`: A file-like object that implements :meth:`write`.
          - `revision` (optional): Which revision (documents with the same
            filename and different uploadDate) of the file to retrieve.
            Defaults to -1 (the most recent revision).
 
        :Note: Revision numbers are defined as follows:
 
          - 0 = the original stored file
          - 1 = the first revision
          - 2 = the second revision
          - etc...
          - -2 = the second most recent revision
          - -1 = the most recent revision
        """
        gout = self.open_download_stream_by_name(filename, revision)
        for chunk in gout:
            destination.write(chunk)
 
    def rename(self, file_id, new_filename):
        """Renames the stored file with the specified file_id.
 
        For example::
 
          my_db = MongoClient().test
          fs = GridFSBucket(my_db)
          # Get _id of file to rename
          file_id = fs.upload_from_stream("test_file", "data I want to store!")
          fs.rename(file_id, "new_test_name")
 
        Raises :exc:`~gridfs.errors.NoFile` if no file with file_id exists.
 
        :Parameters:
          - `file_id`: The _id of the file to be renamed.
          - `new_filename`: The new name of the file.
        """
        result = self._files.update_one({"_id": file_id},
                                        {"$set": {"filename": new_filename}})
        if not result.matched_count:
            raise NoFile("no files could be renamed %r because none "
                         "matched file_id %i" % (new_filename, file_id))