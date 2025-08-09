/*  vim:expandtab:shiftwidth=2:tabstop=2:smarttab:
 * 
 *  LibMemcached
 *
 *  Copyright (C) 2011 Data Differential, http://datadifferential.com/
 *  Copyright (C) 2006-2009 Brian Aker
 *  All rights reserved.
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are
 *  met:
 *
 *      * Redistributions of source code must retain the above copyright
 *  notice, this list of conditions and the following disclaimer.
 *
 *      * Redistributions in binary form must reproduce the above
 *  copyright notice, this list of conditions and the following disclaimer
 *  in the documentation and/or other materials provided with the
 *  distribution.
 *
 *      * The names of its contributors may not be used to endorse or
 *  promote products derived from this software without specific prior
 *  written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */


#include <libmemcached/common.h>
#include <string.h>
#include <mpi.h>
#include <time.h>

MPI_Request request;
MPI_Status mpi_recv_status;
struct timespec start, end;
#define BILLION 1000000000UL

#ifdef HAVE_SYS_SOCKET_H
# include <sys/socket.h>

#endif

void initialize_binary_request(memcached_instance_st* server, protocol_binary_request_header& header)
{
  server->request_id++;
  header.request.magic= PROTOCOL_BINARY_REQ;
  header.request.opaque= htons(server->request_id);
}

enum memc_read_or_write {
  MEM_READ,
  MEM_WRITE
};

/**
 * Try to fill the input buffer for a server with as much
 * data as possible.
 *
 * @param instance the server to pack
 */
static bool repack_input_buffer(memcached_instance_st* instance)
{
  if (instance->read_ptr != instance->read_buffer)
  {
    /* Move all of the data to the beginning of the buffer so
     ** that we can fit more data into the buffer...
   */
    memmove(instance->read_buffer, instance->read_ptr, instance->read_buffer_length);
    instance->read_ptr= instance->read_buffer;
    instance->read_data_length= instance->read_buffer_length;
  }

  /* There is room in the buffer, try to fill it! */
  if (instance->read_buffer_length != MEMCACHED_MAX_BUFFER)
  {
    do {
      /* Just try a single read to grab what's available */
      ssize_t nr;
      if ((nr= ::recv(instance->fd,
                      instance->read_ptr + instance->read_data_length,
                      MEMCACHED_MAX_BUFFER - instance->read_data_length,
                      MSG_NOSIGNAL)) <= 0)
      {
        if (nr == 0)
        {
          memcached_set_error(*instance, MEMCACHED_CONNECTION_FAILURE, MEMCACHED_AT);
        }
        else
        {
          switch (get_socket_errno())
          {
          case EINTR:
            continue;

#if EWOULDBLOCK != EAGAIN
          case EWOULDBLOCK:
#endif
          case EAGAIN:
#ifdef __linux
          case ERESTART:
#endif
            break; // No IO is fine, we can just move on

          default:
            memcached_set_errno(*instance, get_socket_errno(), MEMCACHED_AT);
          }
        }

        break;
      }
      else // We read data, append to our read buffer
      {
        instance->read_data_length+= size_t(nr);
        instance->read_buffer_length+= size_t(nr);

        return true;
      }
    } while (false);
  }

  return false;
}

/**
 * If the we have callbacks connected to this server structure
 * we may start process the input queue and fire the callbacks
 * for the incomming messages. This function is _only_ called
 * when the input buffer is full, so that we _know_ that we have
 * at least _one_ message to process.
 *
 * @param instance the server to star processing iput messages for
 * @return true if we processed anything, false otherwise
 */
static bool process_input_buffer(memcached_instance_st* instance)
{
  /*
   ** We might be able to process some of the response messages if we
   ** have a callback set up
 */
  if (instance->root->callbacks != NULL)
  {
    /*
     * We might have responses... try to read them out and fire
     * callbacks
   */
    memcached_callback_st cb= *instance->root->callbacks;

    memcached_set_processing_input((Memcached *)instance->root, true);

    char buffer[MEMCACHED_DEFAULT_COMMAND_SIZE];
    Memcached *root= (Memcached *)instance->root;
    memcached_return_t error= memcached_response(instance, buffer, sizeof(buffer), &root->result);

    memcached_set_processing_input(root, false);

    if (error == MEMCACHED_SUCCESS)
    {
      for (unsigned int x= 0; x < cb.number_of_callback; x++)
      {
        error= (*cb.callback[x])(instance->root, &root->result, cb.context);
        if (error != MEMCACHED_SUCCESS)
        {
          break;
        }
      }

      /* @todo what should I do with the error message??? */
    }
    /* @todo what should I do with other error messages?? */
    return true;
  }

  return false;
}

static memcached_return_t io_wait(memcached_instance_st* instance,
                                  const short events)
{
#if ENABLE_PRINT
  printf("libmemcached/io.cc - io_wait()\n");
#endif
  /*
   ** We are going to block on write, but at least on Solaris we might block
   ** on write if we haven't read anything from our input buffer..
   ** Try to purge the input buffer if we don't do any flow control in the
   ** application layer (just sending a lot of data etc)
   ** The test is moved down in the purge function to avoid duplication of
   ** the test.
 */
  if (events & POLLOUT)
  {
    if (memcached_purge(instance) == false)
    {
      return MEMCACHED_FAILURE;
    }
  }

  struct pollfd fds;
  fds.fd= instance->fd;
  fds.events= events;
  fds.revents= 0;

  if (fds.events & POLLOUT) /* write */
  {
    instance->io_wait_count.write++;
  }
  else
  {
    instance->io_wait_count.read++;
  }

  if (instance->root->poll_timeout == 0) // Mimic 0 causes timeout behavior (not all platforms do this)
  {
    return memcached_set_error(*instance, MEMCACHED_TIMEOUT, MEMCACHED_AT, memcached_literal_param("poll_timeout() was set to zero"));
  }

  size_t loop_max= 5;
  while (--loop_max) // While loop is for ERESTART or EINTR
  {
    int active_fd= poll(&fds, 1, instance->root->poll_timeout);

    if (active_fd >= 1)
    {
      assert_msg(active_fd == 1 , "poll() returned an unexpected number of active file descriptors");
      if (fds.revents & POLLIN or fds.revents & POLLOUT)
      {
        return MEMCACHED_SUCCESS;
      }

      if (fds.revents & POLLHUP)
      {
        return memcached_set_error(*instance, MEMCACHED_CONNECTION_FAILURE, MEMCACHED_AT, 
                                   memcached_literal_param("poll() detected hang up"));
      }

      if (fds.revents & POLLERR)
      {
        int local_errno= EINVAL;
        int err;
        socklen_t len= sizeof (err);
#if ENABLE_PRINT
        printf("libmemcached/io.cc - io_wait() getsockopt call\n");
#endif
        if (getsockopt(instance->fd, SOL_SOCKET, SO_ERROR, (char*)&err, &len) == 0)
        {
          if (err == 0) // treat this as EINTR
          {
            continue;
          }
          local_errno= err;
        }
#if ENABLE_PRINT
        printf("libmemcached/io.cc - io_wait() memcached_quit_server call\n");
#endif
        memcached_quit_server(instance, true);
        return memcached_set_errno(*instance, local_errno, MEMCACHED_AT,
                                   memcached_literal_param("poll() returned POLLHUP"));
      }
      
      return memcached_set_error(*instance, MEMCACHED_FAILURE, MEMCACHED_AT, memcached_literal_param("poll() returned a value that was not dealt with"));
    }

    if (active_fd == 0)
    {
      return memcached_set_error(*instance, MEMCACHED_TIMEOUT, MEMCACHED_AT, memcached_literal_param("No active_fd were found"));
    }

    // Only an error should result in this code being called.
    int local_errno= get_socket_errno(); // We cache in case memcached_quit_server() modifies errno
    assert_msg(active_fd == -1 , "poll() returned an unexpected value");
    switch (local_errno)
    {
#ifdef __linux
    case ERESTART:
#endif
    case EINTR:
      continue;

    case EFAULT:
    case ENOMEM:
      memcached_set_error(*instance, MEMCACHED_MEMORY_ALLOCATION_FAILURE, MEMCACHED_AT);

    case EINVAL:
      memcached_set_error(*instance, MEMCACHED_MEMORY_ALLOCATION_FAILURE, MEMCACHED_AT, memcached_literal_param("RLIMIT_NOFILE exceeded, or if OSX the timeout value was invalid"));

    default:
      memcached_set_errno(*instance, local_errno, MEMCACHED_AT, memcached_literal_param("poll"));
    }

    break;
  }

  memcached_quit_server(instance, true);

  if (memcached_has_error(instance))
  {
    return memcached_instance_error_return(instance);
  }

  return memcached_set_error(*instance, MEMCACHED_CONNECTION_FAILURE, MEMCACHED_AT, 
                             memcached_literal_param("number of attempts to call io_wait() failed"));
}









static bool io_flush(memcached_instance_st* instance, const bool with_flush, memcached_return_t& error){
  /*
   ** We might want to purge the input buffer if we haven't consumed
   ** any output yet... The test for the limits is the purge is inline
   ** in the purge function to avoid duplicating the logic..
 */
 //a variable that yedam defined..

  {
    WATCHPOINT_ASSERT(instance->fd != INVALID_SOCKET);

    if (memcached_purge(instance) == false)
    {
      return false;
    }
  }
  char *local_write_ptr= instance->write_buffer;
  size_t write_length= instance->write_buffer_offset;

  error= MEMCACHED_SUCCESS;

  WATCHPOINT_ASSERT(instance->fd != INVALID_SOCKET);
  /* Looking for memory overflows */
#if defined(DEBUG)
  if (write_length == MEMCACHED_MAX_BUFFER)
    WATCHPOINT_ASSERT(instance->write_buffer == local_write_ptr);
  WATCHPOINT_ASSERT((instance->write_buffer + MEMCACHED_MAX_BUFFER) >= (local_write_ptr + write_length));
#endif
  while (write_length){
    WATCHPOINT_ASSERT(instance->fd != INVALID_SOCKET);
    WATCHPOINT_ASSERT(write_length > 0);
    int flags;
    if (with_flush){
      flags= MSG_NOSIGNAL;
    }else{
      flags= MSG_NOSIGNAL|MSG_MORE;
    }

#if ENABLE_SOCKET_FUNCTIONS  
    ssize_t sent_length= ::send(instance->fd, local_write_ptr, write_length, flags);
#endif
    
#if ENABLE_MPI_FUNCTIONS
    int size;
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    int client_size = size/2;
    MPI_Irecv(instance->read_buffer, MEMCACHED_MAX_BUFFER, MPI_CHAR, rank-client_size, 0, MPI_COMM_WORLD, &request);
    MPI_Send(local_write_ptr, write_length, MPI_CHAR, rank-client_size, 0, MPI_COMM_WORLD); //0번째 rank에게 보내기
    //MPI_Send(local_write_ptr, write_length, MPI_CHAR, 0, 0, MPI_COMM_WORLD); //0번째 rank에게 보내기
    ssize_t mpi_sent_length = write_length;
#endif

    int local_errno= get_socket_errno(); // We cache in case memcached_quit_server() modifies errno

#if ENABLE_SOCKET_FUNCTIONS
    if (sent_length == SOCKET_ERROR){
#endif

#if ENABLE_MPI_FUNCTIONS
    if(mpi_sent_length == SOCKET_ERROR){
#endif
#if 0 // @todo I should look at why we hit this bit of code hard frequently
      WATCHPOINT_ERRNO(get_socket_errno());
      WATCHPOINT_NUMBER(get_socket_errno());
#endif
      switch (get_socket_errno())
      {
      case ENOBUFS:
        continue;

#if EWOULDBLOCK != EAGAIN
      case EWOULDBLOCK:
#endif
      case EAGAIN:
        {
          /*
           * We may be blocked on write because the input buffer
           * is full. Let's check if we have room in our input
           * buffer for more data and retry the write before
           * waiting..
         */
          if (repack_input_buffer(instance) or process_input_buffer(instance)){
            continue;
          }
          memcached_return_t rc= io_wait(instance, POLLOUT);
          if (memcached_success(rc)){
            continue;
          }else if (rc == MEMCACHED_TIMEOUT){
            return false;
          }
      
          memcached_quit_server(instance, true);
          error= memcached_set_errno(*instance, local_errno, MEMCACHED_AT);
          return false;
        }
      case ENOTCONN:
      case EPIPE:
      default:
#if ENABLE_PRINT      
        printf("libmemcached/io.cc :: io_flush() call memcached_quit_server()2\n");
#endif          
        memcached_quit_server(instance, true);
        error= memcached_set_errno(*instance, local_errno, MEMCACHED_AT);
        WATCHPOINT_ASSERT(instance->fd == INVALID_SOCKET);
        return false;
      }
    }
#if ENABLE_SOCKET_FUNCTIONS
    instance->io_bytes_sent+= uint32_t(sent_length);

    local_write_ptr+= sent_length;
    write_length-= uint32_t(sent_length);
#endif

#if ENABLE_MPI_FUNCTIONS
    instance->io_bytes_sent+= uint32_t(mpi_sent_length);

    local_write_ptr+= mpi_sent_length;
    write_length-= uint32_t(mpi_sent_length);
#endif
  }

  WATCHPOINT_ASSERT(write_length == 0);
  instance->write_buffer_offset= 0;

  return true; //여기로 들어감
}











memcached_return_t memcached_io_wait_for_write(memcached_instance_st* instance)
{
  return io_wait(instance, POLLOUT);
}

memcached_return_t memcached_io_wait_for_read(memcached_instance_st* instance)
{
  return io_wait(instance, POLLIN);
}

static memcached_return_t _io_fill(memcached_instance_st* instance)
{
#if ENABLE_PRINT 
  printf("libmemcached/io.cc :: _io_fill()\n");
#endif
#if ENABLE_SOCKET_FUNCTIONS
  ssize_t data_read;
#if ENABLE_PRINT 
  printf("libmemcached/io.cc :: _io_fill() data_read 2 : %zu\n", data_read);
#endif
#endif

  #if ENABLE_MPI_FUNCTIONS
    int data_read = 20; //내가 대충 오류 안나게할라고 20으로 해둠
    char mpi_recv_buf[100];
    int rank;
    int size;
    MPI_Status mpi_recv_status;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    int client_size = size/2;

  #endif
  do
  {
#if ENABLE_PRINT 
    printf("libmemcached/io.cc :: _io_fill() recv\n");
#endif

#if ENABLE_SOCKET_FUNCTIONS
      data_read= ::recv(instance->fd, instance->read_buffer, MEMCACHED_MAX_BUFFER, MSG_NOSIGNAL);
      //printf("libmemcached/io.cc :: _io_fill() instance->read_buffer : %s\n", instance->read_buffer);
#endif

#if ENABLE_MPI_FUNCTIONS
    //int err = MPI_Recv(instance->read_buffer, MEMCACHED_MAX_BUFFER, MPI_CHAR, rank-client_size, MPI_ANY_TAG, MPI_COMM_WORLD, &mpi_recv_status);
    //int err = MPI_Recv(instance->read_buffer, MEMCACHED_MAX_BUFFER, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &mpi_recv_status);

    //MPI_Get_count(&mpi_recv_status, MPI_CHAR, &data_read);
#endif

    int local_errno= get_socket_errno(); // We cache in case memcached_quit_server() modifies errno

    if (data_read == SOCKET_ERROR)
    {
#if ENABLE_PRINT 
      printf("libmemcached/io.cc :: data read is SOCKET ERROR\n");
#endif
      switch (get_socket_errno())
      {
      case EINTR: // We just retry
        continue;

      case ETIMEDOUT: // OSX
#if EWOULDBLOCK != EAGAIN
      case EWOULDBLOCK:
#endif
      case EAGAIN:
#ifdef __linux
      case ERESTART:
#endif
        {
          memcached_return_t io_wait_ret;
          if (memcached_success(io_wait_ret= io_wait(instance, POLLIN)))
          {
            continue;
          }

          return io_wait_ret;
        }

        /* fall through */

      case ENOTCONN: // Programmer Error
        WATCHPOINT_ASSERT(0);
      case ENOTSOCK:
        WATCHPOINT_ASSERT(0);
      case EBADF:
        assert_msg(instance->fd != INVALID_SOCKET, "Programmer error, invalid socket");
      case EINVAL:
      case EFAULT:
      case ECONNREFUSED:
      default:
        memcached_quit_server(instance, true);
        memcached_set_errno(*instance, local_errno, MEMCACHED_AT);
        break;
      }

      return memcached_instance_error_return(instance);
    }
    else if (data_read == 0)
    {
#if ENABLE_PRINT 
      printf("libmemcached/io.cc :: data read is 0\n");
#endif
      /*
        EOF. Any data received so far is incomplete
        so discard it. This always reads by byte in case of TCP
        and protocol enforcement happens at memcached_response()
        looking for '\n'. We do not care for UDB which requests 8 bytes
        at once. Generally, this means that connection went away. Since
        for blocking I/O we do not return 0 and for non-blocking case
        it will return EGAIN if data is not immediatly available.
      */
      memcached_quit_server(instance, true);
      return memcached_set_error(*instance, MEMCACHED_CONNECTION_FAILURE, MEMCACHED_AT, 
                                 memcached_literal_param("::rec() returned zero, server has disconnected"));
    }
    instance->io_wait_count._bytes_read+= data_read;
#if ENABLE_PRINT 
    printf("libmemcached/io.cc :: _io_fill() data read : %zu | instance->io_wait_count._bytes_read : %zu\n", data_read, instance->io_wait_count._bytes_read);
#endif
  } while (data_read <= 0);
    clock_gettime(CLOCK_REALTIME, &start);
    int flag = 0;
    while (!flag) {
      MPI_Test(&request, &flag, MPI_STATUS_IGNORE);
      if (!flag) {
        //printf("Rank %d: data not yet arrived\n", rank);
        usleep(10); // 10ms 대기
      }
    }
  clock_gettime(CLOCK_REALTIME, &end);
  unsigned long mpi_recv_timer=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
  printf("mpi_recv_timer[%d] : %lu\n", rank, mpi_recv_timer);
  data_read = strlen(instance->read_buffer);
  instance->io_bytes_sent= 0;
  instance->read_data_length= (size_t) data_read;
  instance->read_buffer_length= (size_t) data_read;
  instance->read_ptr= instance->read_buffer;
#if ENABLE_PRINT 
  printf("libmemcached/io.cc :: _io_fill() instance->read_buffer : %s \n", instance->read_buffer);
  printf("libmemcached/io.cc :: _io_fill() return\n");
#endif

  return MEMCACHED_SUCCESS;
}

memcached_return_t memcached_io_read(memcached_instance_st* instance,
                                     void *buffer, size_t length, ssize_t& nread)
{
#if ENABLE_PRINT 
  printf("libmemcached/io.cc :: memcached_io_read()\n");
#endif
  assert(memcached_is_udp(instance->root) == false);
  assert_msg(instance, "Programmer error, memcached_io_read() recieved an invalid Instance"); // Programmer error
  char *buffer_ptr= static_cast<char *>(buffer);
#if ENABLE_PRINT 
  printf("libmemcached/io.cc :: memcached_io_read()1-1\n");
#endif
  #if ENABLE_SOCKET_FUNCTIONS
  if (instance->fd == INVALID_SOCKET)
  {
#if ENABLE_PRINT 
    printf("libmemcached/io.cc :: memcached_io_read()1-2\n");
#endif
#if 0
    assert_msg(int(instance->state) <= int(MEMCACHED_SERVER_STATE_ADDRINFO), "Programmer error, invalid socket state");
#endif
    return MEMCACHED_CONNECTION_FAILURE;
  }
  #endif
#if ENABLE_PRINT 
   printf("libmemcached/io.cc :: memcached_io_read()2\n");
#endif
  while (length)
  {
#if ENABLE_PRINT 
    printf("libmemcached/io.cc :: memcached_io_read()3\n");
#endif
    if (instance->read_buffer_length == 0)
    {
#if ENABLE_PRINT 
      printf("libmemcached/io.cc :: memcached_io_read()3-1\n");
#endif
      memcached_return_t io_fill_ret;
      if (memcached_fatal(io_fill_ret= _io_fill(instance))) //3-1 까지 출력 후, _io_fill 들어감
      {
#if ENABLE_PRINT 
        printf("libmemcached/io.cc :: memcached_io_read()3-2\n");
#endif
        nread= -1;
        return io_fill_ret;
      }
    }
#if ENABLE_PRINT 
    printf("libmemcached/io.cc :: memcached_io_read()3-3\n");
#endif
    if (length > 1)
    {
#if ENABLE_PRINT 
      printf("libmemcached/io.cc :: memcached_io_read()4\n");
#endif
      size_t difference= (length > instance->read_buffer_length) ? instance->read_buffer_length : length;

      memcpy(buffer_ptr, instance->read_ptr, difference);
      length -= difference;
      instance->read_ptr+= difference;
      instance->read_buffer_length-= difference;
      buffer_ptr+= difference;
      // instance 내부 값 예시
#if ENABLE_PRINT 
      printf("if -  instance->read_buffer_length = %zu\n", instance->read_buffer_length);
      printf("if - instance->read_ptr          = %p\n", instance->read_ptr);

      // 만약 read_ptr 포인터 값을 확인하고 싶다면:
      printf("if - *read_ptr (first byte) = 0x%02x\n", (unsigned char)*instance->read_ptr);
#endif
    }
    else
    {
#if ENABLE_PRINT 
       printf("libmemcached/io.cc :: memcached_io_read()5\n");
#endif
      *buffer_ptr= *instance->read_ptr;
      instance->read_ptr++;
      instance->read_buffer_length--;
      buffer_ptr++;
      // instance 내부 값 예시
#if ENABLE_PRINT 
      printf("else - instance->read_buffer_length = %zu\n", instance->read_buffer_length);
      printf("else -  instance->read_ptr          = %p\n", instance->read_ptr);

      // 만약 read_ptr 포인터 값을 확인하고 싶다면:
      printf("else - *read_ptr (first byte) = 0x%02x\n", (unsigned char)*instance->read_ptr);
#endif
      break;
    }
  }

  nread= ssize_t(buffer_ptr - (char*)buffer);
#if ENABLE_PRINT 
  printf("libmemcached/io.cc :: memcached_io_read() end\n");
#endif
  return MEMCACHED_SUCCESS;
}

memcached_return_t memcached_io_slurp(memcached_instance_st* instance)
{
  assert_msg(instance, "Programmer error, invalid Instance");
  assert(memcached_is_udp(instance->root) == false);

  if (instance->fd == INVALID_SOCKET)
  {
    assert_msg(int(instance->state) <= int(MEMCACHED_SERVER_STATE_ADDRINFO), "Invalid socket state");
    return MEMCACHED_CONNECTION_FAILURE;
  }

  ssize_t data_read;
  char buffer[MEMCACHED_MAX_BUFFER];
  do
  {
    data_read= ::recv(instance->fd, instance->read_buffer, sizeof(buffer), MSG_NOSIGNAL);
    if (data_read == SOCKET_ERROR)
    {
      switch (get_socket_errno())
      {
      case EINTR: // We just retry
        continue;

      case ETIMEDOUT: // OSX
#if EWOULDBLOCK != EAGAIN
      case EWOULDBLOCK:
#endif
      case EAGAIN:
#ifdef __linux
      case ERESTART:
#endif
        if (memcached_success(io_wait(instance, POLLIN)))
        {
          continue;
        }
        return MEMCACHED_IN_PROGRESS;

        /* fall through */

      case ENOTCONN: // Programmer Error
        assert(0);
      case ENOTSOCK:
        assert(0);
      case EBADF:
        assert_msg(instance->fd != INVALID_SOCKET, "Invalid socket state");
      case EINVAL:
      case EFAULT:
      case ECONNREFUSED:
      default:
        return MEMCACHED_CONNECTION_FAILURE; // We want this!
      }
    }
  } while (data_read > 0);

  return MEMCACHED_CONNECTION_FAILURE;
}

static bool _io_write(memcached_instance_st* instance,
                      const void *buffer, size_t length, bool with_flush,
                      size_t& written)
{
  //printf("libmemcached/io.cc :: _io_write() 1");
#if ENABLE_PRINT
  printf("libmemcached/io.cc :: _io_write() \n");
#endif
  assert(instance->fd != INVALID_SOCKET);
  assert(memcached_is_udp(instance->root) == false);

  const char *buffer_ptr= static_cast<const char *>(buffer);
#if ENABLE_PRINT
  printf("libmemcached/io.cc :: _io_write() buffer_ptr : %s\n", buffer_ptr);
#endif

  const size_t original_length= length;

  while (length)
  {
#if ENABLE_PRINT
    printf("libmemcached/io.cc :: _io_write() original_length : %zu\n", original_length);
#endif
  //printf("libmemcached/io.cc :: _io_write() 2\n");
    char *write_ptr;
    size_t buffer_end= MEMCACHED_MAX_BUFFER;
    size_t should_write= buffer_end -instance->write_buffer_offset;
    should_write= (should_write < length) ? should_write : length;

    write_ptr= instance->write_buffer + instance->write_buffer_offset;

    memcpy(write_ptr, buffer_ptr, should_write);
    instance->write_buffer_offset+= should_write;
    buffer_ptr+= should_write;
    length-= should_write;
#if ENABLE_PRINT
    printf("libmemcached/io.cc :: _io_write() write_ptr : %s\n", write_ptr);
    printf("libmemcached/io.cc :: _io_write() buffer_ptr : %s\n", buffer_ptr);
#endif
   // printf("libmemcached/io.cc :: _io_write() 3 instance->write_buffer_offset : %d | buffer_end : %d\n", instance->write_buffer_offset, buffer_end);
    if (instance->write_buffer_offset == buffer_end)
    {
      WATCHPOINT_ASSERT(instance->fd != INVALID_SOCKET);

      memcached_return_t rc;
#if ENABLE_PRINT
      printf("libmemcached/io.cc :: _io_write - io_flush\n");
#endif
     // printf("libmemcached/io.cc :: _io_write - io_flush() 1");
      if (io_flush(instance, with_flush, rc) == false)
      {
        written= original_length -length;
#if ENABLE_PRINT
        printf("libmemcached/io.cc :: _io_write return false\n");
#endif
        return false;
      }
    }
  }

  if (with_flush) //여기로 들어감
  {
    memcached_return_t rc;
    WATCHPOINT_ASSERT(instance->fd != INVALID_SOCKET);
#if ENABLE_PRINT
    printf("libmemcached/io.cc :: _io_write - io_flush2\n");
#endif
  //printf("libmemcached/io.cc :: _io_write() 3\n");
    if (io_flush(instance, with_flush, rc) == false)
    {
      written= original_length -length;

      return false;
    }
  }
#if ENABLE_PRINT
  printf("libmemcached/io.cc :: _io_write return, written : %zu\n", written);
#endif
  written= original_length -length;
  //printf("libmemcached/io.cc :: _io_write() 4\n");
  return true;
}

bool memcached_io_write(memcached_instance_st* instance)
{
#if ENABLE_PRINT
  printf("libmemcached/io.cc - memcached_io_write()\n");
#endif
  size_t written;
  return _io_write(instance, NULL, 0, true, written);
}

//여기 들어감
ssize_t memcached_io_write(memcached_instance_st* instance,
                           const void *buffer, const size_t length, const bool with_flush)
{
  size_t written;
#if ENABLE_PRINT
  printf("libmemcached/io.cc - memcached_io_write()2, written : %zu\n", written);
#endif
  if (_io_write(instance, buffer, length, with_flush, written) == false)
  {
#if ENABLE_PRINT
    printf("libmemcached/io.cc - memcached_io_write() return -1\n");
#endif
    return -1;
  }
#if ENABLE_PRINT
  printf("libmemcached/io.cc - memcached_io_write() return ssize_t(written) : %zu \n", written);
#endif
  return ssize_t(written);
}

bool memcached_io_writev(memcached_instance_st* instance,
                         libmemcached_io_vector_st vector[],
                         const size_t number_of, const bool with_flush)
{
  //printf("libmemcached/io.cc :: memcached_io_writev()1\n");
#if ENABLE_PRINT
  printf("libmemcached/io.cc :: memcached_io_writev()\n");
#endif
  //내가 이해하는 파라미터
  //instance : 서버 정보
  //vector : key,value 정보를 포함한 메세지
  //
  ssize_t complete_total= 0;
  ssize_t total= 0;

  for (size_t x= 0; x < number_of; x++, vector++)
  {
    complete_total+= vector->length;
    if (vector->length)
    { //여기 안에 들어감(maybe set)
#if ENABLE_PRINT
      printf("libmemcached/io.cc :: memcached_io_writev() 2\n");
#endif
      size_t written;
      if ((_io_write(instance, vector->buffer, vector->length, false, written)) == false)
      //여기 안들어가네 ..
      {
#if ENABLE_PRINT
        printf("libmemcached/io.cc :: memcached_io_writev() 3\n");
#endif
        return false;
      }
#if ENABLE_PRINT
      printf("libmemcached/io.cc :: memcached_io_writev() 4\n");
#endif
      total+= written;
    }
  }
  
  if (with_flush)
  { //여기 들어감
#if ENABLE_PRINT
    printf("libmemcached/io.cc :: memcached_io_writev() 5\n");
#endif
    if (memcached_io_write(instance) == false)
    {
      //여기 안들어감
#if ENABLE_PRINT
       printf("libmemcached/io.cc :: memcached_io_writev()6\n");
#endif
      return false;
    }
  }
#if ENABLE_PRINT
  printf("libmemcached/io.cc :: memcached_io_writev() 7\n");
#endif
  //printf("libmemcached/io.cc :: memcached_io_writev() 7\n");
  return (complete_total == total);
}

void memcached_instance_st::start_close_socket()
{
  if (fd != INVALID_SOCKET)
  {
    shutdown(fd, SHUT_WR);
    options.is_shutting_down= true;
  }
}

void memcached_instance_st::reset_socket()
{
  if (fd != INVALID_SOCKET)
  {
    (void)closesocket(fd);
    fd= INVALID_SOCKET;
  }
}

void memcached_instance_st::close_socket()
{
  if (fd != INVALID_SOCKET)
  {
    int shutdown_options= SHUT_RD;
    if (options.is_shutting_down == false)
    {
      shutdown_options= SHUT_RDWR;
    }

    /* in case of death shutdown to avoid blocking at close() */
    if (shutdown(fd, shutdown_options) == SOCKET_ERROR and get_socket_errno() != ENOTCONN)
    {
      WATCHPOINT_NUMBER(fd);
      WATCHPOINT_ERRNO(get_socket_errno());
      WATCHPOINT_ASSERT(get_socket_errno());
    }

    reset_socket();
    state= MEMCACHED_SERVER_STATE_NEW;
  }

  state= MEMCACHED_SERVER_STATE_NEW;
  cursor_active_= 0;
  io_bytes_sent= 0;
  write_buffer_offset= size_t(root and memcached_is_udp(root) ? UDP_DATAGRAM_HEADER_LENGTH : 0);
  read_buffer_length= 0;
  read_ptr= read_buffer;
  options.is_shutting_down= false;
  memcached_server_response_reset(this);

  // We reset the version so that if we end up talking to a different server
  // we don't have stale server version information.
  major_version= minor_version= micro_version= UINT8_MAX;
}

memcached_instance_st* memcached_io_get_readable_server(Memcached *memc, memcached_return_t&)
{
#if ENABLE_PRINT
  printf("libmemcached/io.cc :: memcached_io_get_readable_server()\n");
#endif
#define MAX_SERVERS_TO_POLL 100
  struct pollfd fds[MAX_SERVERS_TO_POLL];
  nfds_t host_index= 0;

  for (uint32_t x= 0; x < memcached_server_count(memc) and host_index < MAX_SERVERS_TO_POLL; ++x)
  {
    memcached_instance_st* instance= memcached_instance_fetch(memc, x);

    if (instance->read_buffer_length > 0) /* I have data in the buffer */
    {
      return instance;
    }

    if (instance->response_count() > 0)
    {
      fds[host_index].events= POLLIN;
      fds[host_index].revents= 0;
      fds[host_index].fd= instance->fd;
      ++host_index;
    }
  }

  if (host_index < 2)
  {
    /* We have 0 or 1 server with pending events.. */
    for (uint32_t x= 0; x< memcached_server_count(memc); ++x)
    {
      memcached_instance_st* instance= memcached_instance_fetch(memc, x);

      if (instance->response_count() > 0)
      {
        return instance;
      }
    }

    return NULL;
  }

  int error= poll(fds, host_index, memc->poll_timeout);
  switch (error)
  {
  case -1:
    memcached_set_errno(*memc, get_socket_errno(), MEMCACHED_AT);
    /* FALLTHROUGH */
  case 0:
    break;

  default:
    for (nfds_t x= 0; x < host_index; ++x)
    {
      if (fds[x].revents & POLLIN)
      {
        for (uint32_t y= 0; y < memcached_server_count(memc); ++y)
        {
          memcached_instance_st* instance= memcached_instance_fetch(memc, y);

          if (instance->fd == fds[x].fd)
          {
            return instance;
          }
        }
      }
    }
  }

  return NULL;
}

/*
  Eventually we will just kill off the server with the problem.
*/
void memcached_io_reset(memcached_instance_st* instance)
{
  memcached_quit_server(instance, true);
}

/**
 * Read a given number of bytes from the server and place it into a specific
 * buffer. Reset the IO channel on this server if an error occurs.
 */
memcached_return_t memcached_safe_read(memcached_instance_st* instance,
                                       void *dta,
                                       const size_t size)
{
  size_t offset= 0;
  char *data= static_cast<char *>(dta);

  while (offset < size)
  {
    ssize_t nread;
    memcached_return_t rc;

    while (memcached_continue(rc= memcached_io_read(instance, data + offset, size - offset, nread))) { };

    if (memcached_failed(rc))
    {
      return rc;
    }

    offset+= size_t(nread);
  }

  return MEMCACHED_SUCCESS;
}

memcached_return_t memcached_io_readline(memcached_instance_st* instance,
                                         char *buffer_ptr,
                                         size_t size,
                                         size_t& total_nr)
{
  //printf("libmemcached/io.cc - memcached_io_readline() 시작싲가싲갓ㅈ기ㅏㅅㅈㄱ\n");
#if ENABLE_PRINT
  printf("libmemcached/io.cc - memcached_io_readline()\n");
#endif
  total_nr= 0;
  bool line_complete= false;

  while (line_complete == false)
  {
    if (instance->read_buffer_length == 0)
    {
      /*
       * We don't have any data in the buffer, so let's fill the read
       * buffer. Call the standard read function to avoid duplicating
       * the logic.
     */
#if ENABLE_PRINT
      printf("libmemcached/io.cc - memcached_io_readline()1\n");
#endif
      ssize_t nread;
      memcached_return_t rc= memcached_io_read(instance, buffer_ptr, 1, nread);
#if ENABLE_PRINT
      printf("libmemcached/io.cc - memcached_io_readline() 1-1, rc : %d\n", rc);
#endif
      if (memcached_failed(rc) and rc == MEMCACHED_IN_PROGRESS)
      {
#if ENABLE_PRINT
        printf("libmemcached/io.cc - memcached_io_readline() 1-2, rc : %d\n", rc);
#endif
        memcached_quit_server(instance, true);
        return memcached_set_error(*instance, rc, MEMCACHED_AT);
      }
      else if (memcached_failed(rc))
      {
#if ENABLE_PRINT
        printf("libmemcached/io.cc - memcached_io_readline() 1-3, rc : %d\n", rc);
#endif
        return rc;
      }

      if (*buffer_ptr == '\n')
      {
        line_complete= true;
      }

      ++buffer_ptr;
      ++total_nr;
    }
#if ENABLE_PRINT
     printf("libmemcached/io.cc - memcached_io_readline()2\n");
#endif
    /* Now let's look in the buffer and copy as we go! */
    while (instance->read_buffer_length and total_nr < size and line_complete == false)
    {
#if ENABLE_PRINT
       printf("libmemcached/io.cc - memcached_io_readline()3\n"); //여기 반복
      //  printf("\t instance->read_buffer_length : %zu\n", instance->read_buffer_length); //여기 반복
      //  printf("\t size : %zu\n", size); //여기 반복
      //  printf("\t instance->read_ptr : %s\n", instance->read_ptr); //여기 반복
#endif
      *buffer_ptr = *instance->read_ptr;
      if (*buffer_ptr == '\n')
      {
#if ENABLE_PRINT
        printf("libmemcached/io.cc - memcached_io_readline()4\n");
#endif
        line_complete = true;
      }
      --instance->read_buffer_length;
      ++instance->read_ptr;
      ++total_nr;
      ++buffer_ptr;
    }
#if ENABLE_PRINT
     printf("libmemcached/io.cc - memcached_io_readline()5\n");
#endif
    if (total_nr == size)
    {
      return MEMCACHED_PROTOCOL_ERROR;
    }
  }
#if ENABLE_PRINT
  printf("libmemcached/io.cc - memcached_io_readline end\n");
#endif
  return MEMCACHED_SUCCESS;
}