#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#define L 10000000
#define m_size 10000
// Каждый элемент транспьютерной матрицы может отправлять данные только соседям
// Если ранг соседа совпадает с рангом процесса, значит соседа нет
// Начало
int left( int rank ){
  if ( rank % 5 == 0) return(rank); else return(rank-1);
}
int right( int rank ){
  if ( (rank + 1) % 5 == 0) return(rank); else return(rank+1);
}
int top( int rank ){
  if ( rank > 4 ) return(rank-5); else return(rank);
}
int bottom( int rank ){
  if ( rank < 20 ) return(rank+5); else return(rank); 
}
void test_one( int rank ){
  printf("Hello: rank %d, left %d, right %d, top %d, bottom %d; \n",rank,left(rank),right(rank),top(rank),bottom(rank));
}
// Конец
// bit_print
void print_bit( int num , int len){
  int tmp = num;
  while (len > 0)
  {
    if ( tmp % 2 == 0){
      printf("0");
    } else {
      printf("1");
    }
    tmp = tmp/2;
    len = len - 1;
  }
}
// Count messages that must got ( priority right->left )

// all vector send us from 0;0 to 4;4 from start to end
void get_path( int rank,int** path_array,int* path_count,int* len, int path, int deep ){
  bool left_receive = left(rank) != rank;
  bool top_receive = top(rank) != rank;
  //printf("LEN:%d,R:%d,LR:%d,TR:%d\n",*len,rank,left_receive,top_receive) ;
  if (left_receive){
    get_path(left(rank),path_array,path_count,len,(path << 1) + 1,deep + 1);
  }
  if (top_receive){
    get_path(top(rank),path_array,path_count,len,(path << 1) + 0,deep + 1);
  }
  if ( !(left_receive || top_receive)){
    *len = *len + deep; 
    *path_count = *path_count + 1;
    int* tmp = realloc(*path_array,*path_count * sizeof(int));
    *path_array =  tmp;
    tmp[*path_count - 1] = path;
    *path_array = tmp;
  }
}

void path_receive( int* left_p,int* top_p, int path, int len){
  int curr_count_message;
  *left_p = 0;
  *top_p = 0;
  int local = 0;
  int entry = 0;
  int curr_rank = 0;
  if ( L % m_size == 0) curr_count_message = L / m_size; else curr_count_message = L / m_size + 1;
  while (len > 0)
  {
    int offset = curr_count_message % 2;
    if( path % 2 == 0 ){
      bool right_send = right(curr_rank) != curr_rank;
      bool bottom_send = bottom(curr_rank) != curr_rank;
      curr_rank = curr_rank + 5;
      if ( right_send & bottom_send){
        //local = local + curr_count_message / 2;
        curr_count_message = curr_count_message / 2;
      }
    } else {
      bool right_send = right(curr_rank) != curr_rank;
      bool bottom_send = bottom(curr_rank) != curr_rank;
      curr_rank = curr_rank + 1;
      if ( right_send & bottom_send){
        //local = local + curr_count_message / 2;
        curr_count_message = curr_count_message / 2 + offset;
      }
      //local = local + curr_count_message / 2 + offset;
      //curr_count_message = curr_count_message / 2 + offset;
    }
    if ( len == 1){
      entry = path % 2;
    }
    path = path / 2;
    len = len -1 ;
    //printf(" : %d ",curr_count_message);
  }
  if ( entry == 1 ){
    *left_p = *left_p + curr_count_message;
  } else {
    *top_p = *top_p + curr_count_message;
  }
}
// test
void test_two( int rank ){
  if ( rank == 6){
    int* paths = malloc(sizeof(int));
    int paths_leng = 0;
    int paths_count = 0;
    get_path(6,&paths,&paths_count,&paths_leng,0,0);
    printf("LENGTH_ONE_PATH:: %d\n",paths_leng/paths_count);
    int summ = 0;
    for (int i = 0; i < paths_count; i++)
    {
      int top_rcv = 0;
      int left_rcv = 0;
      print_bit(paths[i],paths_leng/paths_count);
      path_receive(&left_rcv,&top_rcv,paths[i],paths_leng/paths_count);
      summ = summ + top_rcv + left_rcv;
      printf("::: LEFT %d, TOP %d, ALL %d \n",left_rcv,top_rcv,top_rcv + left_rcv);
    }
    printf("RESULT:::%d\n",summ);
    free(paths);
  }
}
// create tag function
int create_tag ( int path , int number ){
  return(number * 256 + path);
}



int main(int argc, char** argv) {
  // Initialize program
  MPI_Init(NULL, NULL);
  int rank;
  int world;
  int buff_size;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &world);
  MPI_Pack_size(L,MPI_INT,MPI_COMM_WORLD,&buff_size); 
  buff_size = buff_size + 1000000; // На всякий случай
  int* buff = malloc(buff_size);
  MPI_Buffer_attach(buff,buff_size);
  // Open send path
  bool right_send = right(rank) != rank ;
  bool bottom_send = bottom(rank) != rank ;
  // Open receive path
  bool left_receive = left(rank) != rank;
  bool top_receive = top(rank) != rank;
  // Tag = path (from 0 to current block right = 1 bottom = 0 ) + number_message
  // Fot path 8 last bit 
  // small version for matrix 2x2
  if ( rank == 0){
    // Generik test big send array
    int package_count =  ( L % m_size == 0) ? L / m_size : L / m_size + 1;
    int*pointers[package_count];
    for (int i = 0; i < package_count; i++)
    {
      pointers[i] = malloc(m_size * sizeof(int));
    }
    // switcher transport some message right, some message bottom
    int bt_cn = 0;
    int rg_cn = 0;
    bool switcher = true;
    // Важно в последнем сообщении будет в конце содержаться мусор,
    // поэтому важно это учитывать и доводить L до размера кратного 
    // m_size 
    for (int i = 0; i < package_count; i++)
    {
      if (switcher){
        MPI_Bsend(pointers[i], m_size, MPI_INT, right(rank), create_tag(1,rg_cn) ,
                MPI_COMM_WORLD);
        //printf("0_right ::: ready send %d \n",create_tag(1,rg_cn));
        rg_cn = rg_cn + 1;
        switcher = !switcher;
      } else {
        MPI_Bsend(pointers[i], m_size, MPI_INT, bottom(rank), create_tag(0,bt_cn),
                MPI_COMM_WORLD);
        //printf("0_down ::: ready send %d \n",create_tag(0,bt_cn));
        bt_cn = bt_cn + 1;
        switcher = !switcher;
      }
    }
    for (int i = 0; i < package_count; i++)
    {
      free(pointers[i]);
    }
  } else {
    int* top_recv  = malloc(sizeof(int) * m_size);
    int* left_recv = malloc(sizeof(int) * m_size);
    bool left_path = left(rank) != rank;
    bool right_path = right(rank) != rank;
    bool top_path = top(rank) != rank;
    bool bottom_path = bottom(rank) != rank;
    bool switcher = true;
    bool left_recieve_cont = true;
    bool top_receieve_cont = true;
    int* paths = malloc(sizeof(int));
    int paths_leng = 0;
    int paths_count = 0;
    get_path(rank,&paths,&paths_count,&paths_leng,0,0);
    bool switchs[paths_count];
    for (int i = 0; i < paths_count; i++)
    {
      switchs[i] = true;
    }
    int length = paths_leng/paths_count;
    int left_path_iterator = 0;
    int top_path_iterator = 0;
    int left_rcv_counter = 0;
    int top_rcv_counter = 0;
    MPI_Request left_mpi_req;
    MPI_Request  top_mpi_req;
    MPI_Status left_mpi_status;
    MPI_Status top_mpi_status;
    int right_send[paths_count];
    for (int i = 0; i < paths_count; i++)
    {
      right_send[i] = 0;
    }
    int bottom_send[paths_count];
    for (int i = 0; i < paths_count; i++)
    {
      bottom_send[i] = 0;
    }
    int summ = 0;
    

    // Отлаживать будем на 7 процессе (2;3)
    while ( left_recieve_cont || top_receieve_cont )
    {
      // Проверяем актуален ли текущий путь для левого потока
      // Если нет пытаемся найти путь, который будет актуален
      // актуаленый путь - путь с которого мы еще можем получить пакеты
      if ( left_recieve_cont  ){
        int cc_left = 0;
        int cc_top = 0;
        path_receive(&cc_left,&cc_top,paths[left_path_iterator],length);
        if ( left_rcv_counter >= cc_left ){
          left_rcv_counter = 0;
          int tmp = left_path_iterator;
          left_path_iterator = -1;
          for (int i = tmp + 1; i < paths_count; i++)
          {
            cc_left = 0;
            cc_top = 0;
            path_receive(&cc_left,&cc_top,paths[i],length);
            if ( cc_left != 0){
              left_path_iterator = i;
              break;
            } 
          }
          if ( left_path_iterator == -1){
            left_recieve_cont = false;
          }
        } 
      }
      //Аналогично для для верхнего потока
      if ( top_receieve_cont ){
        int cc_left = 0;
        int cc_top = 0;
        path_receive(&cc_left,&cc_top,paths[top_path_iterator],length);
        if ( top_rcv_counter >= cc_top ){
          top_rcv_counter = 0;
          int tmp = top_path_iterator;
          top_path_iterator = -1;
          for (int i = tmp + 1; i < paths_count; i++)
          {
            cc_left = 0;
            cc_top = 0;
            path_receive(&cc_left,&cc_top,paths[i],length);
            if ( cc_top != 0){
              top_path_iterator = i;
              break;
            } 
          }
          if ( top_path_iterator == -1){
            top_receieve_cont = false;
          }
        } 
      }

      // Теперь инициализируем получение сообщений от соседей

      #define debug_rcv 0
      #define debug_send 0

      if( left_recieve_cont ){
         MPI_Irecv(left_recv, m_size ,MPI_INT, left(rank)
         , create_tag(paths[left_path_iterator],left_rcv_counter),
          MPI_COMM_WORLD, &left_mpi_req);
      }

      if( top_receieve_cont ){
         MPI_Irecv(top_recv, m_size ,MPI_INT, top(rank)
         , create_tag(paths[top_path_iterator],top_rcv_counter),
          MPI_COMM_WORLD, &top_mpi_req);
      }

      // // Дождемся сообщения

      if ( left_recieve_cont ){
        if ( rank == debug_rcv){
          printf(">LEFT_WAIT_%d, i wait from path %d, message number %d; or all_path %d \n",rank,paths[left_path_iterator],left_rcv_counter,create_tag(paths[left_path_iterator],left_rcv_counter));
        }
        MPI_Wait(&left_mpi_req,&left_mpi_status);
        left_rcv_counter = left_rcv_counter + 1;
        if ( rank == debug_rcv){
          printf(">>LEFT_GOT_%d, i got from path %d, message number %d; or all_path %d \n",rank,paths[left_path_iterator],left_rcv_counter,create_tag(paths[left_path_iterator],left_rcv_counter-1));
        }
      }

      // top_rcv_counter = top_rcv_counter + 1;
      // left_rcv_counter = left_rcv_counter + 1;

      if ( top_receieve_cont ){
        if ( rank == debug_rcv){
          printf(">>>TOP_WAIT_%d, i wait from path %d, message number %d; or all_path %d \n",rank,paths[top_path_iterator],top_rcv_counter,create_tag(paths[top_path_iterator],top_rcv_counter));
        }
        MPI_Wait(&top_mpi_req,&top_mpi_status);
        top_rcv_counter = top_rcv_counter + 1;
        if ( rank == debug_rcv){
          printf(">>>>TOP_GOT_%d, i got from path %d, message number %d; or all_path %d \n",rank,paths[top_path_iterator],top_rcv_counter,create_tag(paths[top_path_iterator],top_rcv_counter-1));
        }
      }
      // Отправим сообщения дальше по конвееру
      if ( left_recieve_cont){
        // Все пути отправки закрыты значит мы в 24 процессе
        if( !right_path & !bottom_path){
          summ = summ + 1;
          printf("FINAL::: path: ,");
          print_bit(paths[left_path_iterator],length);
          printf(" num_packet: %d, summ: %d\n",left_rcv_counter,summ);
        } else {
          // Значит есть путь передачи сообщений, найдем его
          if (!right_path) switchs[left_path_iterator] = false;
          if (!bottom_path) switchs[left_path_iterator] = true;
          // switcher === true отправляем направо 
          // switcher === false отправляем вниз 
          if (switchs[left_path_iterator]){
            MPI_Bsend(left_recv, m_size, MPI_INT, right(rank), create_tag( (1 << length ) + paths[left_path_iterator],right_send[left_path_iterator]),
              MPI_COMM_WORLD);
              if ( rank == debug_send){
                printf("++LEFT_RIGHT_%d i want send %d\n",rank,create_tag( (1 << length ) + paths[left_path_iterator],right_send[left_path_iterator]));
              }
            right_send[left_path_iterator] = right_send[left_path_iterator] + 1;
            switchs[left_path_iterator] = !switchs[left_path_iterator];
          } else {
            MPI_Bsend(left_recv, m_size, MPI_INT, bottom(rank), create_tag( (0 << length ) + paths[left_path_iterator],bottom_send[left_path_iterator]),
              MPI_COMM_WORLD);
              if ( rank == debug_send){
                printf("++LEFT_BOTTOM_%d i want send %d\n",rank,create_tag( (0 << length ) + paths[left_path_iterator],bottom_send[left_path_iterator]));
              }
            bottom_send[left_path_iterator] = bottom_send[left_path_iterator] + 1;
            switchs[left_path_iterator] = !switchs[left_path_iterator];
          }
        }
      } 
      if (top_receieve_cont ){
          // Все пути отправки закрыты значит мы в 24 процессе
        if( !right_path & !bottom_path){
          summ = summ + 1;
          printf("FINAL::: path: ,");
          print_bit(paths[top_path_iterator],length);
          printf(" num_packet: %d, summ : %d\n",top_rcv_counter,summ);
        } else {
          // Повторим тоже для второго пакета ( пришедшего сверху)
          if (!right_path) switchs[top_path_iterator] = false;
          if (!bottom_path) switchs[top_path_iterator] = true;
          // switcher === true отправляем направо 
          // switcher === false отправляем вниз 
          if (switchs[top_path_iterator]){
            MPI_Bsend(top_recv, m_size, MPI_INT, right(rank), create_tag( (1 << length ) + paths[top_path_iterator],right_send[top_path_iterator]),
              MPI_COMM_WORLD);
            if ( rank == debug_send){
              printf("+++TOP_RIGHT_%d i want send %d\n",rank,create_tag( (1 << length) + paths[top_path_iterator],right_send[top_path_iterator]));
            }
            right_send[top_path_iterator] = right_send[top_path_iterator] + 1;
            switchs[top_path_iterator] = !switchs[top_path_iterator];
          } else {
            MPI_Bsend(top_recv, m_size, MPI_INT, bottom(rank), create_tag( (0 << length) + paths[top_path_iterator],bottom_send[top_path_iterator]),
              MPI_COMM_WORLD);
            if ( rank == debug_send){
              printf("+++TOP_DOWN_%d i want send %d\n",rank,create_tag( (0 << length) + paths[top_path_iterator],bottom_send[top_path_iterator]));
            }
            bottom_send[top_path_iterator] = bottom_send[top_path_iterator] + 1;
            switchs[top_path_iterator] = !switchs[top_path_iterator];
          }
        }
      }
    }
    free(left_recv);
    free(top_recv);
    free(paths);
  }
  // Finalize program
  MPI_Buffer_detach(buff,&buff_size);
  free(buff);
  printf(" %d , die...\n",rank);
  MPI_Finalize();
}