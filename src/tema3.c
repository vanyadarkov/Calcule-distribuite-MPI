#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

// M -> master
// W -> worker

#define MAX_FILE_NAME 32
#define NR_MASTERS 4
#define M_W_TAG 1
#define M_M_TAG 0
#define DISC 1
#define PART 2
#define DISTR 0

/**
 * @brief  Trimite un mesaj catre un proces
 * @note   
 * @param  buffer: ce trimitem
 * @param  size: dimensiunea
 * @param  datatype: tipul de date
 * @param  rank: cine trimite
 * @param  dest: cui trimite
 * @param  tag: tag-ul mesajului
 * @retval None
 */
void send_message(void * buffer, int size, MPI_Datatype datatype, int rank, int dest, int tag) {
    MPI_Send(buffer, size, datatype, dest, tag, MPI_COMM_WORLD);
    printf("M(%d,%d)\n", rank, dest);
}

/**
 * @brief  Structura care va memora indicii de inceput si sfarsit al unui sub-array dintr-un array mai mare
 * @note   
 * @retval None
 */
typedef struct _sent_array_indices_ {
    int start;
    int end;
} Sent_array_indices;

/**
 * @brief  Citeste workerii pentru coordonator si trimite mesaj catre workeri rankul sau
 * @note   
 * @param  rank: rankul coordonatorului
 * @param  nr_workers: (handle) numarul de workeri
 * @retval array cu rankul workerilor
 */
int * get_workers(int rank, int * nr_workers) {
    char cluster_file_name[MAX_FILE_NAME];
    sprintf(cluster_file_name, "cluster%d.txt", rank);
    FILE* cluster_fp = fopen(cluster_file_name, "r");
    fscanf(cluster_fp, "%d", nr_workers);
    int* workers = calloc(*nr_workers, sizeof(int));
    for(int i = 0; i < *nr_workers; i++) {
        fscanf(cluster_fp, "%d", &workers[i]);
        send_message(&rank, 1, MPI_INT, rank, workers[i], M_W_TAG);
    }
    fclose(cluster_fp);
    return workers;
}

/**
 * @brief  Intoarce numarul de workeri per master (coordonator)
 * @note   
 * @param  topology: topologia
 * @param  nr_procs: numarul de procese
 * @param  rank: rank-ul pentru care cautam
 * @retval numarul de workeri pentru coordonatorul cu rankul rank din topologie
 */
int get_workers_per_master(int * topology, int nr_procs, int rank) {
    int res = 0;
    for(int j = 4; j < nr_procs; j++) {
        if(topology[rank * nr_procs + j]) {
            res++;
        }
    }
    return res;
}

/**
 * @brief  afisarea topologiei
 * @note   
 * @param  topology: topologia
 * @param  nr_procs: numarul de procesare
 * @param  rank: rankul procesului care cere afisarea topologiei
 * @retval None
 */
void print_topology(int * topology, int nr_procs, int rank) {
    printf("%d ->", rank);
    for(int i = 0; i < NR_MASTERS; i++) {
        int printed_first = 0;
        for(int j = NR_MASTERS; j < nr_procs; j++) {
            if(topology[i * nr_procs + j]) {
                if(printed_first) {
                    printf(",%d", j);
                } else {
                    printf(" %d:%d", i, j);
                    printed_first = 1;
                }
            }
        }
    }
    printf("\n");
}

int main(int argc, char ** argv) {
    int  nr_procs, rank;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nr_procs);
    MPI_Comm_rank(MPI_COMM_WORLD,&rank);
    int active_clusters = NR_MASTERS;
    int communication_error = atoi(argv[2]);
    if(communication_error != PART) communication_error = DISC;
    else active_clusters--;
    int N = atoi(argv[1]);
    int nr_workers;
    int master_rank = rank;
    int topology[NR_MASTERS][nr_procs];

    // Init topologie
    for(int i = 0; i < NR_MASTERS; i++) {
        for(int j = 0; j < nr_procs; j++) {
            topology[i][j] = 0;
        }
    }
    // Coordonatori
    if(rank < NR_MASTERS) {
        int cicle_start = DISTR;
        int cicle_end = communication_error;
        int * workers = get_workers(rank, &nr_workers);
        // Completeaza topologia
        for(int i = 0; i < nr_workers; i++) {
            topology[rank][workers[i]] = 1;
        }
        // Indici pentru vecini
        int prev_neigh, next_neigh = -1;
        prev_neigh = rank - 1 < DISTR ? NR_MASTERS - 1 : rank - 1;
        next_neigh = rank + 1 == NR_MASTERS ? DISTR : rank + 1;
        topology[rank][prev_neigh] = 1;
        topology[rank][next_neigh] = 1;
        if(communication_error == PART) {
            if(prev_neigh == 1 || prev_neigh == DISTR) {
                topology[rank][prev_neigh] = 0;
                prev_neigh = -1;
            }
            if(next_neigh == 1 || next_neigh == 2) {
                topology[rank][next_neigh] = 0;
                next_neigh = -1;
            }
        }
        // topologie se trimite de la 0 counterclockwise
        if(rank == cicle_start) {
            // Trimitem topologia noastra catre precedentul
            send_message(&(topology[0][0]), NR_MASTERS * nr_procs, MPI_INT, rank, prev_neigh, M_M_TAG);
            int recv_topology[NR_MASTERS * nr_procs];
            // Primim topologia completa de la precedentul
            MPI_Recv(recv_topology, NR_MASTERS * nr_procs, MPI_INT, prev_neigh, M_M_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            for(int i = 0; i < NR_MASTERS; i++) {
                for(int j = 0; j < nr_procs; j++) {
                    topology[i][j] = recv_topology[i * nr_procs +j];
                }
            }
        } else {
            // daca nu se intra pe if-ul asta -> e partitionare
            if(communication_error != PART || (communication_error == PART && rank != 1)) {
                // Primim topologia de la vecinul urmator (3 de la 0, 2 de la 3..)
                int recv_topology[NR_MASTERS * nr_procs];
                MPI_Recv(recv_topology, NR_MASTERS * nr_procs, MPI_INT, next_neigh, M_M_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                // Completam topologia noastra cu topologia vecinului
                for(int i = 0; i < NR_MASTERS; i++) {
                    if(i == rank) continue;
                    for(int j = 0; j < nr_procs; j++) {
                        topology[i][j] = recv_topology[i * nr_procs + j];
                    }
                }
                if(rank != cicle_end) {
                    // Trimitem topologia cunoscuta vecinului precedent (3 -> 2, 2 -> 1...)
                    send_message(&(topology[0][0]), NR_MASTERS * nr_procs, MPI_INT, rank, prev_neigh, M_M_TAG);
                    // Primim topologia completa (suprascriem pe a noastra)
                    MPI_Recv(recv_topology, NR_MASTERS * nr_procs, MPI_INT, prev_neigh, M_M_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    for(int i = 0; i < active_clusters; i++) {
                        for(int j = 0; j < nr_procs; j++) {
                            topology[i][j] = recv_topology[i * nr_procs + j];
                        }
                    }
                }
                // trimitem topologia completa catre urmatorul vecin
                send_message(&(topology[0][0]), NR_MASTERS * nr_procs, MPI_INT, rank, next_neigh, M_M_TAG);
            }
        }
        // Afisarea topologiei
        print_topology(&(topology[0][0]), nr_procs, rank);
        // Distribuirea topologiei catre workeri
        for(int i = 0; i < nr_workers; i++) {
            send_message(&(topology[0][0]), NR_MASTERS * nr_procs, MPI_INT, rank, workers[i], M_W_TAG);
        }
        if(rank == cicle_start) {
            // Genereaza array-ul pentru calcule
            int work[N];
            for(int i = 0; i < N; i++) {
                work[i] = N - i - 1;
            }
            // Se calculeaza si se trimite pentru fiecare coordonator partea sa de array
            int active_workers = 0;
            // numarul de workeri activi
            for(int i = 0; i < NR_MASTERS; i++) {
                for(int j = NR_MASTERS; j < nr_procs; j++) {
                    if(topology[i][j]) active_workers++;
                }
            }
            int per_worker = N / active_workers;
            int remainder = N - per_worker * active_workers;
            Sent_array_indices cluster_indices[active_clusters];
            int first = 0;
            int last = per_worker * get_workers_per_master(&(topology[0][0]), nr_procs, rank);
            cluster_indices[0].start = first;
            cluster_indices[0].end = last;
            for(int i = 0; i < nr_workers; i++) {
                send_message(&per_worker, 1, MPI_INT, 0, workers[i], M_W_TAG);
                send_message(&work[i * per_worker], per_worker, MPI_INT, 0, workers[i], M_W_TAG);
            }
            // pentru fiecare coordonator
            for(int i = cicle_end; i < NR_MASTERS; i++) {
                first = last;
                int workers_per_master = get_workers_per_master(&(topology[0][0]), nr_procs, i);
                last = first + per_worker * workers_per_master;
                // aici, in caz ca avem rest dupa impartirea N / W echilibram
                for(int j = 1; j <= workers_per_master && remainder > 0; j++)
                {
                    last += per_worker;
                    remainder -= per_worker;
                    if(remainder < 0) last += remainder;
                }
                cluster_indices[i].start = first;
                cluster_indices[i].end = last;
                // trimitem catre precedent al cui este aceasta parte de array
                send_message(&i, 1, MPI_INT, rank, prev_neigh, M_M_TAG);
                int to_send = last - first;
                // dimensiunea
                send_message(&to_send, 1, MPI_INT, rank, prev_neigh, M_M_TAG);
                // arrayul
                send_message(&work[first], to_send, MPI_INT, rank, prev_neigh, M_M_TAG);
            }
            // asamblare de la workerii cicle_end (0)
            for(int i = 0; i < nr_workers; i++) {
                MPI_Recv(&work[i * per_worker], per_worker, MPI_INT, workers[i], M_W_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
            // primim de la fiecare cluster activ partea sa de array
            for(int i = 1; i < active_clusters; i++) {
                // de la cine am primit
                int from;
                MPI_Recv(&from, 1, MPI_INT, prev_neigh, prev_neigh, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                // cat primim
                int size;
                MPI_Recv(&size, 1, MPI_INT, prev_neigh, from, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                // scriem in array initial ceea ce am primit de la coordonatori
                MPI_Recv(&work[cluster_indices[from].start], size, MPI_INT, prev_neigh, from, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
            printf("Rezultat: ");
            for(int i = 0; i < N; i++) {
                printf("%d ", work[i]);
            }
            printf("\n");
        } else {
            if(communication_error != PART || (communication_error == PART && rank != 1)) {
                int * array; 
                int array_size;
                int to_receive = rank;
                if(communication_error == PART) to_receive--;
                // primeste bucatile de array de la urmatorul vecin si daca e nevoie trimitem catre urmatorul
                for(int i = 0; i < to_receive; i++) {
                    int dest_rank;
                    MPI_Recv(&dest_rank, 1, MPI_INT, next_neigh, M_M_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    if(dest_rank == rank) {
                        MPI_Recv(&array_size, 1, MPI_INT, next_neigh, M_M_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                        array = calloc(array_size, sizeof(int));
                        MPI_Recv(array, array_size, MPI_INT, next_neigh, M_M_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    } else {
                        int size;
                        MPI_Recv(&size, 1, MPI_INT, next_neigh, M_M_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                        int arr[size];
                        MPI_Recv(arr, size, MPI_INT, next_neigh, M_M_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                        send_message(&dest_rank, 1, MPI_INT, rank, prev_neigh, M_M_TAG);
                        send_message(&size, 1, MPI_INT, rank, prev_neigh, M_M_TAG);
                        send_message(arr, size, MPI_INT, rank, prev_neigh, M_M_TAG);
                    }
                }

                // Distribuim array-ul catre fiecare worker
                int per_worker = array_size / nr_workers;
                Sent_array_indices workers_indices[nr_workers];
                int remainder = array_size - per_worker * nr_workers;
                int i = 0;
                // distribuim restul echilibrat catre workeri
                for(int start = 0, end = per_worker; start < array_size; start = end, end = start + per_worker) {
                    if(remainder > 0) {
                        end++;
                        remainder--;
                    }
                    workers_indices[i].start = start;
                    workers_indices[i++].end = end;
                }
                // trimitem catre fiecare worker
                for(int i = 0; i < nr_workers; i++) {
                    int to_send = workers_indices[i].end - workers_indices[i].start;
                    send_message(&to_send, 1, MPI_INT, rank, workers[i], M_W_TAG);
                    send_message(&array[workers_indices[i].start], to_send, MPI_INT, rank, workers[i], M_W_TAG);
                }
                // primim
                for(int i = 0; i < nr_workers; i++) {
                    int to_recv = workers_indices[i].end - workers_indices[i].start;
                    MPI_Recv(&array[workers_indices[i].start], to_recv, MPI_INT, workers[i], M_W_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                }
                
                // Trimitem rezultatele noastre
                send_message(&rank, 1, MPI_INT, rank, next_neigh, rank);
                send_message(&array_size, 1, MPI_INT, rank, next_neigh, rank);
                send_message(array, array_size, MPI_INT, rank, next_neigh, rank);

                // Primim rezultatele de la vecinii precedenti si propagam catre urmatorul
                for(int i = 0; i < to_receive - 1; i++) {
                    int from;
                    MPI_Recv(&from, 1, MPI_INT, prev_neigh, prev_neigh, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    int size;
                    MPI_Recv(&size, 1, MPI_INT, prev_neigh, from, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    int array[size];
                    MPI_Recv(array, size, MPI_INT, prev_neigh, from, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    send_message(&from, 1, MPI_INT, rank, next_neigh, rank);
                    send_message(&size, 1, MPI_INT, rank, next_neigh, from);
                    send_message(array, size, MPI_INT, rank, next_neigh, from);
                }
                free(array);
            }
        }
    } else {
        MPI_Status status;
        MPI_Recv(&master_rank, 1, MPI_INT, MPI_ANY_SOURCE, M_W_TAG, MPI_COMM_WORLD, &status);
        int recv_topology[NR_MASTERS * nr_procs];
        // Primim topologia completa
        {
            MPI_Recv(recv_topology, NR_MASTERS * nr_procs, MPI_INT, master_rank, M_W_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            for(int i = 0; i < NR_MASTERS; i++) {
                for(int j = 0; j < nr_procs; j++) {
                    topology[i][j] = recv_topology[i * nr_procs +j];
                }
            }
            print_topology(&(topology[0][0]), nr_procs, rank);
        }
        // Daca nu cumva master e 1 si avem partitie (sa nu asteptam sa primim array pentru calcule)
        if(communication_error != PART || (communication_error == PART && master_rank != 1)) {
            int array_size;
            MPI_Recv(&array_size, 1, MPI_INT, master_rank, M_W_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            int array[array_size];
            MPI_Recv(array, array_size, MPI_INT, master_rank, M_W_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            for(int i = 0; i < array_size; i++) {
                array[i] *= 5;
            }
            send_message(array, array_size, MPI_INT, rank, master_rank, M_W_TAG);
        }
    }

    MPI_Finalize();
}

