/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_SLAVE_H_
#define SSDB_SLAVE_H_

#include <stdint.h>
#include <string>
#include <pthread.h>
#include <vector>
#include "ssdb/ssdb_impl.h"
#include "ssdb/binlog.h"
#include "net/link.h"

class ReqlogFile {
	private:
		int write_fd;
		int read_fd;

		volatile int64_t write_offset;
		volatile int64_t write_file_index;
		volatile int64_t write_count;
		volatile int64_t read_offset;
		volatile int64_t read_file_index;
		volatile int64_t read_count;
		volatile int64_t last_seq;
		std::string last_key;
		volatile uint64_t copy_count;
		volatile uint64_t sync_count;

		std::string work_dir;
		uint64_t req_max_file_size;

	public:
		int64_t get_last_seq(){
			return last_seq;
		}
		std::string get_last_key(){
			return last_key;
		}
		ReqlogFile(const std::string& dir){
			write_fd = -1;
			read_fd = -1;
			copy_count = 0;
			sync_count = 0;
			work_dir = dir;
			req_max_file_size = 0;
			read_offset = 0;
			read_file_index = 0;
			read_count = 0;
			write_offset = 0;
			write_file_index = 0;
			write_count = 0;
			last_seq = 0;
			last_key = "";

		};
		int init();
		int read_req_from_file(std::vector<Bytes> *req);
		int proc(const std::vector<Bytes> &req);
		std::string reqlog_filename(int index){
			return work_dir + "/" + "reqlog." + str(index);
		}
		~ReqlogFile(){
			close(write_fd);
			close(read_fd);
			log_debug("~ReqlogFile");
		}
};

class Slave{
private:
	uint64_t last_seq;
	std::string last_key;
	uint64_t copy_count;
	uint64_t sync_count;
		
	std::string id_;

	SSDB *ssdb;
	SSDB *meta;
	Link *link;
	std::string master_ip;
	int master_port;
	bool is_mirror;
	char log_type;

	static const int DISCONNECTED = 0;
	static const int INIT = 1;
	static const int COPY = 2;
	static const int SYNC = 4;
	static const int OUT_OF_SYNC = 8;
	int status;

	ReqlogFile *reqlog;
	bool use_reqlog;
	std::string work_dir;


	void migrate_old_status();

	std::string status_key();
	void load_status();
	void save_status();

	volatile bool thread_quit;
	pthread_t run_thread_tid;
	static void* _run_thread(void *arg);
		
	pthread_t reqlog_thread_tid;
	static void* _reqlog_thread(void* arg);


	int proc(const std::vector<Bytes> &req);
	int proc_noop(const Binlog &log, const std::vector<Bytes> &req);
	int proc_copy(const Binlog &log, const std::vector<Bytes> &req);
	int proc_sync(const Binlog &log, const std::vector<Bytes> &req);

	unsigned int connect_retry;
	int connect();
	bool connected(){
		return link != NULL;
	}
public:
	std::string auth;
	Slave(SSDB *ssdb, SSDB *meta, const char *ip, int port, bool is_mirror=false);
	~Slave();
	void start();
	void stop();
		
	void set_id(const std::string &id);
	std::string stats() const;
	void set_work_dir(const std::string &dir){
		if(dir == ""){
			work_dir = "./";
		}else{
			work_dir = dir;
		}
	}
	void set_use_reqlog(bool on_off){
		use_reqlog = on_off;
	}

};

#endif
