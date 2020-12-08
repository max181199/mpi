#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>
#include <mpi.h>

double bench_t_start, bench_t_end;

static double rtclock()
{
  struct timeval Tp;
  int stat;
  stat = gettimeofday(&Tp, NULL);
  if (stat != 0)
    printf("Error return from gettimeofday: %d", stat);
  return (Tp.tv_sec + Tp.tv_usec * 1.0e-6);
}

void bench_timer_start()
{
  bench_t_start = rtclock();
}

void bench_timer_stop()
{
  bench_t_end = rtclock();
}

void bench_timer_print()
{
  printf("Time in seconds = %0.6lf\n", bench_t_end - bench_t_start);
}

#define MAXSIZE 5
#define CHECKPOINT_STEP 1
float Matrix[MAXSIZE][MAXSIZE];
float MatrixCopy[MAXSIZE][MAXSIZE];


void FillMatrix(float Matrix[MAXSIZE][MAXSIZE], int start, int stop){
  for (int i = 0; i < MAXSIZE; ++i)
  {
    for (int j = 0; j < MAXSIZE; ++j)
    {
      Matrix[i][j] = 0;
    }
  }

  // for (int i = start; i <= stop; ++i)
  // {
  //   for (int j = 0; j < MAXSIZE; ++j)
  //   {
  //     //Matrix[i][j] = i+j+i*j+(i+1)/(j+1) - (i+1)%(j+1);// RESULT TEST [3,4,5,6,7] Matrix
  //     if (i == j)
  //       Matrix[i][j] = 1;
  //     else
  //       Matrix[i][j] = 0; // TIME TEST
  //                         //Matrix[i][j] = MAXSIZE*i+j+1;					 // Another Result test
  //                         //Matrix[i][j]=0;							    // Funny TEst
  //   }
  // }
  Matrix[0][0]=-2;Matrix[0][1]= 5;Matrix[0][2]= 0;Matrix[0][3]=-1;Matrix[0][4]= 3;
  Matrix[1][0]= 1;Matrix[1][1]= 0;Matrix[1][2]= 3;Matrix[1][3]= 7;Matrix[1][4]=-2;
  Matrix[2][0]= 3;Matrix[2][1]=-1;Matrix[2][2]= 0;Matrix[2][3]= 5;Matrix[2][4]=-5;
  Matrix[3][0]= 2;Matrix[3][1]= 6;Matrix[3][2]=-4;Matrix[3][3]= 1;Matrix[3][4]= 2;
  Matrix[4][0]= 0;Matrix[4][1]=-3;Matrix[4][2]=-1;Matrix[4][3]= 2;Matrix[4][4]= 3;
}

void PrintMatrix(float Matrix[][MAXSIZE], int start, int stop){
  for (int i = start; i <= stop; ++i)
  {
    printf("\n");
    printf("%d ::: ", i);
    for (int j = 0; j < MAXSIZE; ++j)
    {
      printf("%f    ", Matrix[i][j]);
    }
  }
  printf("\n");
}

int Sort(int minus[MAXSIZE]){
  int count = 0;
  for (int i = 0; i < MAXSIZE; ++i)
  {
    for (int j = 0; j < MAXSIZE - 1; ++j)
    {
      if (minus[j] > minus[j + 1])
      {
        int a = minus[j];
        minus[j] = minus[j + 1];
        minus[j + 1] = a;
        count++;
      }
    }
  }
  return count;
}

int Round(int i, int rank, int count){
  int key = rank;
  while (key < i)
  {
    key += count;
  }
  return key;
}

void ReplaceRow(float * changeRow , float * replaceRow, int size){
  for (int i = 0; i < size; i++)
  {
    changeRow[i] = replaceRow[i];
  }
}


int main()
{
  MPI_Init(NULL, NULL);
  int rank;
  int count;
  float RESULT = 1;
  MPI_Status stat;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &count);
  int minus[MAXSIZE] = {0};

  // Нужен для подсчета 
  int small = 0;
  // Worklist строки которые обрабатывает данный процесс
  // Является массивом элементом которого является номер процесса обработчика
  // Длина массива кол-во строк, номер строки соответсвует номеру элемента массива
  // Считаем что все процессы живы
  int worklist[MAXSIZE] = {0};
  for (int i = 0; i < MAXSIZE; i++)
  {
    worklist[i] = i % count;
  }
  // Массив статусов всех процессов, по умолчанию считаем
  // Что все процессы живы
  int* status = malloc(count * sizeof(int));
  for (int i = 0; i < count; i++)
  {
    status[i] = 1;
  }
  
  int my_status = 1;
  // Кол-во нерабочих процессов ( если кол-во изменилось, то нужно возвращаться на шаг назад)
  int broken = 0;
  // Пустая строка для синхронизации матрицы
  float row[MAXSIZE] = {0};  
  //!!! Будем считать, что если у процесса произошел сбой, то он имеет статус 0
  //!!! И на запросы о проверке статуса отвечает значением 0
  //!!! Считаем, что процессы со сбоем более не учавствуют в работе
  int real_iter = -1;

  bench_timer_start();

  FillMatrix(Matrix,    0,MAXSIZE - 1);
  FillMatrix(MatrixCopy,0,MAXSIZE - 1);

  
  for (int i = 0; i < MAXSIZE; ++i)
  {
    real_iter = real_iter + 1;
    // BEGIN SAVEPOINT
    // Сохраним текущее состояние матрицы
    // Необходимо собрать актуальное состояние каждой строки с каждого процесса
    // Довольно Трудозатратно
    // т.е. чтобы каждый процесс имел актуальную матрицу
    //if(rank==2 && real_iter==6 && status[rank]!=0){PrintMatrix(Matrix,0,MAXSIZE-1);}
    //if( rank==0 && real_iter==3 && status[rank]!=0){PrintMatrix(MatrixCopy,0,MAXSIZE-1);}

  
    if ( status[rank] != 0){
      if ( i % CHECKPOINT_STEP == 0 ){
        // Синхронизация матриц
        // Шаг первый соберем все строки в корневои процессе
        if (worklist[i] == rank){
          for (int k = 0; k < MAXSIZE; k++){
            if ( worklist[k] != rank ){
              MPI_Recv(Matrix[k], MAXSIZE, MPI_FLOAT ,worklist[k], 20, MPI_COMM_WORLD, NULL); 
            }
          }
        } else {
          for (int z = 0; z < MAXSIZE; z++){
            if ( worklist[z] == rank){
              MPI_Send(Matrix[z], MAXSIZE, MPI_FLOAT, worklist[i], 20, MPI_COMM_WORLD);
            }
          } 
        }
        // Шаг второй передадим матрицу всем не корневым процессам
        for (int j = 0; j < MAXSIZE; j++)
        {
          if (worklist[i] == rank)
          {
            for (int k = 0; k < count; k++)
            {
              if( k!=worklist[i] && status[k]!=0){
                MPI_Send(Matrix[j], MAXSIZE, MPI_FLOAT, k, 21, MPI_COMM_WORLD);
              }
            } 
          } else {
            MPI_Recv(Matrix[j], MAXSIZE, MPI_FLOAT ,worklist[i], 21, MPI_COMM_WORLD, NULL); 
          }
        }

        //if(rank==0 && real_iter==6){PrintMatrix(Matrix,0,MAXSIZE-1);}
        
        // Теперь сохраним строки, которые потенциально может изменить
        // текущий процесс в MatrixCopy

        for (int j = 0; j < MAXSIZE; j++)
        {
          if ( worklist[j]==rank){
            ReplaceRow(MatrixCopy[j],Matrix[j],MAXSIZE);
          }
        }
        

        //if(rank==0 && real_iter==0){PrintMatrix(Matrix,0,MAXSIZE-1);}
    
        // т.е в копии матрицы каждого процесса лежат строки, которые будут изменятся
        // пи сбоем нам необходимо восстановить эти строки у каждого процесса

        // Хочется чтобы на момент чекпоинта все процессы имели общий minus
        // Необходимо для конечных вычислений
        // Погибнуть может любой => каждый должен иметь все реальные минусы
        // Идея корневой процесс должен получить со всех
        // Далее корневой процесс должен отправить всем
        // Пусть корневой процесс worklist[i], status[worklist[i]] != 0  
        // В силу построения  worklist и в силу локальности мест сбоя
        
        // Part one - All to Root
          if ( rank == worklist[i]){
            int tmp_minus[MAXSIZE] = {0};
            for (int z = 0; z < count; z++)
            {
              if ( status[z] != 0 && z != rank){
                //printf("IWANT %d\n",z);
                MPI_Recv(tmp_minus, MAXSIZE, MPI_INT,z, 2, MPI_COMM_WORLD, NULL); 
                for (int i = 0; i < MAXSIZE; i++)
                {
                  minus[i] = tmp_minus[i] == 0 ? minus[i] : tmp_minus[i];
                }  
              }
            }
          } else {
            if (status[rank]!=0){
              //printf("ISEND %d\n",rank);
              MPI_Send(minus, MAXSIZE, MPI_INT, worklist[i], 2, MPI_COMM_WORLD);
            }
          }
        // Part two - root to all with replace
          if ( rank == worklist[i]){
            for (int z = 0; z < count; z++)
            {
              if ( status[z] != 0 && z != rank){
                MPI_Send(minus, MAXSIZE, MPI_INT, z, 3, MPI_COMM_WORLD);
              }     
            }
          } else {
            if (status[rank]!=0){
              //printf("ISEND %d\n",rank);
              MPI_Recv(minus, MAXSIZE, MPI_INT,worklist[i], 3, MPI_COMM_WORLD, NULL); 
            }
          }
        // END SAVEPOINT
    }


      // Config OUR matrix
      if (rank == worklist[i])
      {
        for (int k = 0; k < count; ++k)
        {
          if (k != worklist[i])
          {
            MPI_Send(&Matrix[i], MAXSIZE, MPI_FLOAT, k, 0, MPI_COMM_WORLD);
          }
        } // можно попытаться сделать неблокирующую но проблема будет при синхронизации
          // хотя можно и попробовать будет интересно

        // BEGIN BROKE_BLOKE THERE PROCESS DIE  
          if ( i==1 && rank==0){
            my_status = 0;
          }
        // END BROKE_BLOCK

        int maxj = 0;
        float maxel = 0;
        float cand;
        for (int j = 0; j < MAXSIZE; j++)
        {
          cand = Matrix[i][j];
          if (Matrix[i][j] < 0)
            cand = cand * (-1);
          if (cand > maxel)
          {
            maxel = cand;
            maxj = j;
          }
        }
        RESULT = RESULT * Matrix[i][maxj];
        //printf("I AM %d ; AND I MULTIPLAY %f ;\n",rank,Matrix[i][maxj] );
        minus[maxj] = i + 1;

        float kf = 0;
        for (int k = 0; k < MAXSIZE; k++)
        {
          if ( worklist[k] == worklist[i] && k != i ){
            kf = Matrix[k][maxj] / Matrix[i][maxj];
            for (int j = 0; j < MAXSIZE; ++j)
            {
              Matrix[k][j] = Matrix[k][j] - (Matrix[i][j] * kf);
            }
          }
        }
      }
      else
      {
        //printf("ITR : %d , me %d\n",real_iter,rank);
        
        MPI_Recv(&Matrix[i], MAXSIZE, MPI_FLOAT, worklist[i], 0, MPI_COMM_WORLD, &stat);
        // BEGIN BROKE_BLOKE THERE PROCESS DIE  
          if ( i==1 && rank==0){
            my_status = 0;
          }
        // END BROKE_BLOCK
        int maxj = 0;
        float maxel = 0;
        float cand;
        for (int j = 0; j < MAXSIZE; j++)
        {
          cand = Matrix[i][j];
          if (Matrix[i][j] < 0)
            cand = cand * (-1);
          if (cand > maxel)
          {
            maxel = cand;
            maxj = j;
          }
        }

        float kf = 0;
        for (int k = 0 ; k < MAXSIZE; k++)
        {
          if ( worklist[k] == rank && k > i){
            kf = Matrix[k][maxj] / Matrix[i][maxj];
            for (int j = 0; j < MAXSIZE; ++j)
            {
              Matrix[k][j] = Matrix[k][j] - (Matrix[i][j] * kf);
            }
          }
        }
      }
    }

    if ( (i % CHECKPOINT_STEP) == (CHECKPOINT_STEP-1)){
      //  BEGIN BREAKPOINT
        int* ttmp = malloc(count * sizeof(int));
        for (int i = 0; i < count; i++)
        {
          ttmp[i] = my_status;
        } 
        //printf("IWANT %d ITER %d\n",rank,real_iter);
        MPI_Alltoall(ttmp,1,MPI_INT,status,1,MPI_INT,MPI_COMM_WORLD);
        free(ttmp);
        // Необходимо синхронизировать кол-во ответов у сломанных процессов
        if ( status[rank] == 0 ){
          int tmp = 0;
          for (int k = 0; k < count; k++)
          {
            tmp = status[k] + tmp;
          }
          tmp = count - tmp;
          if ( tmp != broken ){
            broken = tmp;
            i = i - 1;  
          }
        }
      // BEGIN RECOVERY
      if ( status[rank] != 0){
        // COUNTING NUMBER OF BROKE
          int tmp = 0;
          for (int k = 0; k < count; k++)
          {
            tmp = status[k] + tmp;
          }
          tmp = count - tmp;
          // IF BROKE ANOTHER ONE...
            if ( tmp != broken ){
              broken = tmp;
              // Отменим все вычисления минуса, которые мы сделали
              if ( rank == worklist[i]){
                for (int h = 0; h < count; h++)
                {
                  if ( minus[h] == (i + 1) ){
                    minus[h] = 0;
                  }
                }
              }
              // Вернем вернем матрицу из копии сделаем шаг назад
                i = i - CHECKPOINT_STEP;
                for (int z = 0; z < MAXSIZE; z++)
                {
                  if ( worklist[z] == rank ){
                    ReplaceRow(Matrix[z],MatrixCopy[z],MAXSIZE);
                  }
                }
              // Пересчитаем worklist для каждого процесса
              if ( tmp == count){
                printf("ALL_PROCESS_DIE!!!!!\n");
                break;
              } else {
                for (int k = 0; k < MAXSIZE; k++)
                {
                  if ( 0 == status[worklist[k]]){
                    while(status[small] == 0){
                      small = (small + 1)%count;
                    }
                    worklist[k] = small;
                    small = (small + 1)%count;   
                  }
                }
              }
            } 
            // printf("I %d,br_count %d,iteration %d, my_status: %d, status: ",rank,tmp,real_iter,my_status);
            // for (int i = 0; i < count; i++)
            // {
            //   printf(" %d ",status[i]);
            // }
            // printf("\t");
            // printf("worklist: ");
            // for (int i = 0; i < MAXSIZE; i++)
            // {
            //   printf(" %d ",worklist[i]);
            // }
            // printf("\n");


      }

      //  END RECOVERY
      //  END BREAKPOINT
    }

        if( rank == worklist[i] && status[rank] != 0){
          for (int i = 0; i < MAXSIZE; i++)
            {
              printf(" %d ",worklist[i]);
            }
            printf(" __ %d \n",real_iter);
        }
    //if(rank==0){PrintMatrix(Matrix,0,MAXSIZE-1);}
  }

  //printf("EXCELENT %d \n",rank);

  // Теперь мы не знаем какие процессы живы
  // Выбираем живой процесс с наименьшим рангом 
  // Подсчитываем result и отправляем живому процессу с наименьшим рангом
  // Хотябы один должен существовать !!!
  // Если не существует то будем считать, что программа совершила ошибку

  // Каждый процесс должен посчитать ранг сборщика

  // if(rank==0){
  //   PrintMatrix(Matrix,0,MAXSIZE-1);
  //   printf("I %d - minus : ",rank);
  //   for (size_t i = 0; i < MAXSIZE; i++)
  //   {
  //     printf(" %d ",minus[i] );
  //   }
  //   printf("\n");
  // }
  //if(rank==0){PrintMatrix(Matrix,0,MAXSIZE-1);}

  if ( status[rank] != 0){
    int root = -1;
    int life_count = 0; // Кол-во живых процессов;
    for (int i = 0; i < count; i++)
    {
      if ( status[i] != 0 && root == -1){
        root = i;
      }
      if (status[i] !=0){
        life_count = life_count + 1;
      }
    }
    if ( root == -1){
      printf("ALL_PROCCES_FAULT :) \n");
    } else {
      //printf ( "I %d ROOT %d LIFE %d\n",rank,root,life_count);
    }

    // Все живые процессы подсчитывают result
    // minus известен !!!
    RESULT = 1;

    for (int i = 0; i < MAXSIZE; i++)
    {
      if ( worklist[i] == rank ){
        // Значит что этот процесс обработал i строку 
        // и в его матрице лежит нужное значение
        for (int j = 0; j < MAXSIZE; j++)
        {
          if ( (minus[j] - 1) == i){
            // На пересечении j столбца и i=minus[j]-1 строки 
            // лежит максимальное значение в строке
            RESULT = RESULT * Matrix[i][j];
          }
        }
      }
    }

   // printf("I %d AND MY RESULT %f\n",rank,RESULT);


    // Рутовый процесс должен собрать со всех RESULT и minuss
    // Объединить и вывести на печать


    if ( rank == root){
      float payback = 1;
      int tmp_minus[MAXSIZE] = {0};
      for (int i = 0; i < count; i++)
      {
        if (status[i] != 0 && i != root)
        {
          MPI_Recv(&payback, 1, MPI_FLOAT, i, 4, MPI_COMM_WORLD, NULL);
          RESULT = RESULT * payback;
          //printf(" I got %f from %d \n",RESULT,i);
          MPI_Recv(tmp_minus, MAXSIZE, MPI_INT, i, 4, MPI_COMM_WORLD, NULL);
          for (int i = 0; i < MAXSIZE; ++i)
          {
            minus[i] = tmp_minus[i]==0?minus[i]:tmp_minus[i];
          }
        } 
      }

      // Для отладки :) 
      // printf("I_ROOOT %d - minus : ",rank);
      // for (size_t i = 0; i < count; i++)
      // {
      //   printf(" %d ",minus[i] );
      // }
      // printf(" AND MY MATRIX ::: ");
      // PrintMatrix(Matrix,0,MAXSIZE-1);

    } else {
      MPI_Send(&RESULT, 1, MPI_FLOAT, root, 4, MPI_COMM_WORLD);
      MPI_Send(&minus, MAXSIZE, MPI_INT, root, 4, MPI_COMM_WORLD);
      // Для отладки :) 
      // printf("I %d - minus : ",rank);
      // for (size_t i = 0; i < count; i++)
      // {
      //   printf(" %d ",minus[i] );
      // }
      // printf(" AND MY MATRIX ::: ");
      // PrintMatrix(Matrix,0,MAXSIZE-1);
    }
    if ( rank == root ){
      int c = Sort(minus);
      printf("SORT=%d\n", c);
      if (c % 2)
      {
        printf("RESULT: %f\n", RESULT * -1);
      }
      else
      {
        printf("RESULT: %f\n", RESULT);
      }
      bench_timer_stop();
      bench_timer_print();
      MPI_Finalize();
    } else {
      bench_timer_stop();
      free(status);
      MPI_Finalize();
    }
  } else {
    bench_timer_stop();
    free(status);
    MPI_Finalize();
  }
}
