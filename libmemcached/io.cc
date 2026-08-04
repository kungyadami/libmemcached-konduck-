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

MPI_Request request;

static bool mpi_buffer_contains(const char *buffer, size_t length,
                                const char *needle, size_t needle_length)
{
  if (needle_length == 0 || length < needle_length)
  {
    return false;
  }

  for (size_t i= 0; i + needle_length <= length; i++)
  {
    if (memcmp(buffer + i, needle, needle_length) == 0)
    {
      return true;
    }
  }
  return false;
}

static bool mpi_request_expects_response(const char *buffer, size_t length,
                                         bool with_flush)
{
  if (with_flush == false)
  {
    return false;
  }
  if (mpi_buffer_contains(buffer, length, " noreply", 8))
  {
    return false;
  }
  if (length >= 4 && memcmp(buffer, "quit", 4) == 0)
  {
    return false;
  }
  return true;
}

static ssize_t mpi_find_crlf(const char *buffer, size_t max_length)
{
  for (size_t i= 0; i + 1 < max_length; i++)
  {
    if (buffer[i] == '\r' && buffer[i + 1] == '\n')
    {
      return (ssize_t)i;
    }
  }
  return -1;
}

static ssize_t mpi_find_token_end(const char *buffer, size_t max_length,
                                  const char *token, size_t token_length)
{
  if (token_length == 0 || max_length < token_length)
  {
    return -1;
  }

  for (size_t i= 0; i + token_length <= max_length; i++)
  {
    if (memcmp(buffer + i, token, token_length) == 0)
    {
      return (ssize_t)(i + token_length);
    }
  }
  return -1;
}

static size_t mpi_parse_response_bytes(const char *buffer, size_t max_length)
{
  if (max_length == 0)
  {
    return 0;
  }

  if (max_length >= 5 && memcmp(buffer, "VALUE", 5) == 0)
  {
    ssize_t value_end= mpi_find_token_end(buffer, max_length, "END\r\n", 5);
    return value_end > 0 ? (size_t)value_end : max_length;
  }

  ssize_t line_end= mpi_find_crlf(buffer, max_length);
  return line_end >= 0 ? (size_t)line_end + 2 : max_length;
}

static int mpi_complete_previous_send(memcached_instance_st *instance)
{
  if (instance->mpi_send_pending == false)
  {
    return MPI_SUCCESS;
  }

  int err= MPI_Wait(&instance->mpi_send_request, MPI_STATUS_IGNORE);
  if (err == MPI_SUCCESS)
  {
    instance->mpi_send_pending= false;
    instance->mpi_send_request= MPI_REQUEST_NULL;
  }
  return err;
}

extern "C" void get_wait_reset(void)
{
}

extern "C" void get_wait_print(void)
{
  int rank= -1;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  printf("[client_round_trip] rank=%d count=%lu total_time=%lu avg_time=%lu\n",
         rank,
         0UL,
         0UL,
         0UL);
  fflush(stdout);
}
MPI_Status mpi_recv_status;

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
    printf("client repack_input_buffer, instance->read_buffer_length : %d | instance->read_buffer : %s\n", instance->read_buffer_length, instance->read_buffer);
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
  //printf("최종 io_flush1\n");
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
      //printf("최종 io_flush2\n");
#if ENABLE_SOCKET_FUNCTIONS  
    ssize_t sent_length= ::send(instance->fd, local_write_ptr, write_length, flags);
#endif
    
#if ENABLE_MPI_FUNCTIONS
    int size;
    int rank;
      //printf("최종 io_flush3\n");
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
      //printf("최종 io_flush4\n");
    int client_size = size/2;
    //printf("client, [%d]에게 전송 메세지1 : %s\n",target_server, local_write_ptr);
    //bool is_get_request= (write_length >= 3 && strncmp(local_write_ptr, "get", 3) == 0);
    bool expects_response= mpi_request_expects_response(local_write_ptr, write_length,with_flush);
    if (instance->mpi_recv_pending){
      error= memcached_set_error(*instance, MEMCACHED_IN_PROGRESS, MEMCACHED_AT);
      return false;
    }
    if (mpi_complete_previous_send(instance) != MPI_SUCCESS){
      error= memcached_set_error(*instance, MEMCACHED_WRITE_FAILURE, MEMCACHED_AT);
      return false;
    }
    //printf("client, [%d]에게 전송 메세지2 : %s\n",target_server, local_write_ptr);
    if (write_length > MEMCACHED_MAX_BUFFER){
      error= memcached_set_error(*instance, MEMCACHED_WRITE_FAILURE, MEMCACHED_AT);
      return false;
    }
#if ENABLE_MPI_FIXED_PADDING
    memset(instance->mpi_send_buffer, 0, MEMCACHED_MAX_BUFFER);
#endif
    memcpy(instance->mpi_send_buffer, local_write_ptr, write_length);
#if ENABLE_MPI_FIXED_PADDING
    const size_t mpi_send_length= MEMCACHED_MAX_BUFFER;
#else
    const size_t mpi_send_length= write_length;
#endif

#if ENABLE_MPI_ASYNC
  //printf("[client send] instance->mpi_send_buffer : %s\n", instance->mpi_send_buffer);
  int send_err= MPI_Isend(instance->mpi_send_buffer, mpi_send_length, MPI_CHAR,  target_server, SEND_TAG, MPI_COMM_WORLD, &instance->mpi_send_request);
  if (send_err != MPI_SUCCESS){
    error= memcached_set_error(*instance, MEMCACHED_WRITE_FAILURE, MEMCACHED_AT);
    return false;
  }
  instance->mpi_send_pending= true;
  if (expects_response){
    int recv_err= MPI_Irecv(instance->mpi_recv_buffer, MEMCACHED_MAX_BUFFER,  MPI_CHAR, target_server, RECV_TAG, MPI_COMM_WORLD,  &instance->mpi_recv_request);
    if (recv_err != MPI_SUCCESS){
      mpi_complete_previous_send(instance);
      error= memcached_set_error(*instance, MEMCACHED_READ_FAILURE, MEMCACHED_AT);
      return false;
    }
    instance->mpi_recv_pending= true;
  }
#else
    //printf("[client send] target_server : %d, 내 rank : %d\n",target_server, rank);
    int send_err= MPI_Send(instance->mpi_send_buffer, mpi_send_length, MPI_CHAR, 0, SEND_TAG, MPI_COMM_WORLD);
    if (send_err != MPI_SUCCESS)
    {
      error= memcached_set_error(*instance, MEMCACHED_WRITE_FAILURE, MEMCACHED_AT);
      return false;
    }
#endif
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
#endif
  #if ENABLE_MPI_FUNCTIONS
    int data_read; //내가 대충 오류 안나게할라고 20으로 해둠
    char mpi_recv_buf[100];
    int rank;
    int size;
    MPI_Status mpi_recv_status;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    int client_size = size/2;
  #if ENABLE_PRINT 
    printf("libmemcached/io.cc :: _io_fill() recv1\n");
#endif
  #endif
  do{
#if ENABLE_PRINT 
    printf("libmemcached/io.cc :: _io_fill() recv2\n");
#endif

#if ENABLE_SOCKET_FUNCTIONS
      data_read= ::recv(instance->fd, instance->read_buffer, MEMCACHED_MAX_BUFFER, MSG_NOSIGNAL);
      //printf("libmemcached/io.cc :: _io_fill() instance->read_buffer : %s\n", instance->read_buffer);
#endif
#if ENABLE_PRINT 
    printf("libmemcached/io.cc :: _io_fill() recv3\n");
#endif
#if ENABLE_MPI_FUNCTIONS
#if ENABLE_MPI_ASYNC
    #if ENABLE_PRINT 
    printf("libmemcached/io.cc :: _io_fill() recv, MPI_Waitall3??\n");
#endif
    if (instance->mpi_recv_pending == false){
      return memcached_set_error(*instance, MEMCACHED_READ_FAILURE, MEMCACHED_AT);
    }
    #if ENABLE_PRINT 
    printf("libmemcached/io.cc :: _io_fill() recv, MPI_Waitall??\n");
#endif
    if (instance->mpi_send_pending){
#if ENABLE_PRINT
      printf("libmemcached/io.cc :: _io_fill() wait send\n");
#endif
      int send_err= MPI_Wait(&instance->mpi_send_request, MPI_STATUS_IGNORE);
      if (send_err != MPI_SUCCESS){
        return memcached_set_error(*instance, MEMCACHED_WRITE_FAILURE, MEMCACHED_AT);
      }
      instance->mpi_send_pending= false;
      instance->mpi_send_request= MPI_REQUEST_NULL;
    }

#if ENABLE_PRINT
    printf("libmemcached/io.cc :: _io_fill() wait recv\n");
#endif
    MPI_Status recv_status;
    int recv_err= MPI_Wait(&instance->mpi_recv_request, &recv_status);
    if (recv_err != MPI_SUCCESS){
      return memcached_set_error(*instance, MEMCACHED_READ_FAILURE, MEMCACHED_AT);
    }
    instance->mpi_recv_pending= false;
    instance->mpi_recv_request= MPI_REQUEST_NULL;

    size_t count= mpi_parse_response_bytes(instance->mpi_recv_buffer, MEMCACHED_MAX_BUFFER);
    if (count > MEMCACHED_MAX_BUFFER){
      return memcached_set_error(*instance, MEMCACHED_READ_FAILURE, MEMCACHED_AT);
    }
    memcpy(instance->read_buffer, instance->mpi_recv_buffer, count);
    //printf("[client] recv[%d] \"%s\" \n", mpi_recv_status.MPI_SOURCE, instance->read_buffer);
    //printf("client, [%d]에게 받은 메세지, \"%s\"\n", mpi_recv_status.MPI_SOURCE, instance->read_buffer);
    //MPI_Get_count(&mpi_recv_status, MPI_CHAR, &data_read);
    data_read = count;
#else
    //printf("[client] MPI_Recv()1\n");
    MPI_Status recv_status;
    //printf("[client] MPI_Recv()\n");
    int err= MPI_Recv(instance->read_buffer, MEMCACHED_MAX_BUFFER, MPI_CHAR, 0, RECV_TAG, MPI_COMM_WORLD, &recv_status);
    if (err != MPI_SUCCESS)
    {
      return memcached_set_error(*instance, MEMCACHED_READ_FAILURE, MEMCACHED_AT);
    }
    //printf("[client] MPI_Recv()2\n");
    size_t count= mpi_parse_response_bytes(instance->read_buffer, MEMCACHED_MAX_BUFFER);
    //printf("[client] MPI_Recv()3, count : %d\n", count);
    if (count > MEMCACHED_MAX_BUFFER)
    {
      return memcached_set_error(*instance, MEMCACHED_READ_FAILURE, MEMCACHED_AT);
    }
    data_read= (ssize_t)count;
#endif
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
  instance->io_bytes_sent= 0;
  instance->read_data_length= (size_t) data_read;
  instance->read_buffer_length= (size_t) data_read;
  instance->read_ptr= instance->read_buffer;
#if ENABLE_PRINT 
  printf("libmemcached/io.cc :: _io_fill() instance->read_buffer : %s \n", instance->read_buffer);
  printf("libmemcached/io.cc :: _io_fill() return\n");
#endif
  //memset(instance->read_buffer, 0, MEMCACHED_MAX_BUFFER);
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
      printf("if - instance->read_ptr          = %s\n", instance->read_ptr);

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
#if ENABLE_PRINT
  printf("libmemcached/io.cc :: _io_write() , instance->fd : %d\n", instance->fd);
#endif

#if ENABLE_SOCKET_FUNCTIONS
  assert(instance->fd != INVALID_SOCKET);
  assert(memcached_is_udp(instance->root) == false);
#endif
#if ENABLE_PRINT
  printf("libmemcached/io.cc :: _io_write()2 \n");
#endif
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
    char *write_ptr;
    size_t buffer_end= MEMCACHED_MAX_BUFFER;
    size_t should_write= buffer_end -instance->write_buffer_offset;
    should_write= (should_write < length) ? should_write : length;

    write_ptr= instance->write_buffer + instance->write_buffer_offset;

    memcpy(write_ptr, buffer_ptr, should_write);
    instance->write_buffer_offset+= should_write;
    buffer_ptr+= should_write;
    length-= should_write;
    if (instance->write_buffer_offset == buffer_end)
    {
      WATCHPOINT_ASSERT(instance->fd != INVALID_SOCKET);

      memcached_return_t rc;
#if ENABLE_PRINT
      printf("libmemcached/io.cc :: _io_write() io_flush\n");
#endif
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
    if (io_flush(instance, with_flush, rc) == false){
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
#if ENABLE_PRINT
  printf("libmemcached/io.cc :: memcached_io_writev()\n");
#endif
  //내가 이해하는 파라미터
  //instance : 서버 정보
  //vector : key,value 정보를 포함한 메세지
  //set은
  // " set key flag exptime value_length \r\n value \r\n"
  //get은
  // "get key \r\n" 이라, vector[0]이 null이냐 null이 아니냐에 따라 일단 get/set을 나눠볼 수 있을 것 같다.
  ssize_t complete_total= 0;
  ssize_t total= 0;

  for (size_t x= 0; x < number_of; x++, vector++)
  {
    complete_total+= vector->length;
    if (vector->length)
    { //여기 안에 들어감(get은 일단 들어감)
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
      //  printf("libmemcached/io.cc - memcached_io_readline()3\n"); //여기 반복
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
