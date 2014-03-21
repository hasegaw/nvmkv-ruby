#include "ruby.h"
#include <errno.h>
#include <stdbool.h>
#include </usr/include/nvm/nvm_kv.h>

#include <fcntl.h>

#ifdef __cplusplus
#define _EXTERN_C_ extern "C"
#define _RB_WRAP_FUNC_(func) reinterpret_cast<VALUE(*)(...)>(func)
#else
#define _EXTERN_C_ /**/
#define _RB_WRAP_FUNC_(func) func
#endif

VALUE ki_to_hash(nvm_kv_key_info_t* ki) {
	VALUE ret = rb_hash_new();
	rb_hash_aset(ret, rb_str_new2("pool_id"), INT2FIX(ki->pool_id));
	rb_hash_aset(ret, rb_str_new2("key_len"), INT2FIX(ki->key_len));
	rb_hash_aset(ret, rb_str_new2("value_len"), INT2FIX(ki->value_len));
	rb_hash_aset(ret, rb_str_new2("expiry"), LONG2NUM(ki->expiry));
	rb_hash_aset(ret, rb_str_new2("gen_count"), INT2FIX(ki->gen_count));
	rb_hash_aset(ret, rb_str_new2("reserved1"), INT2FIX(ki->reserved1));
	return ret;
}

_EXTERN_C_ VALUE wrap_syscall_open(VALUE self, VALUE filename) {
	Check_Type(filename, T_STRING);

	char *filename_buf = RSTRING_PTR(filename);
	int fd = open(filename_buf, O_RDWR | O_DIRECT);
	return INT2FIX(fd);
}

_EXTERN_C_ VALUE wrap_kv_open(VALUE self, VALUE id, VALUE version, VALUE max_pools, VALUE expiry, VALUE cache_size) {
	Check_Type(id, T_FIXNUM);
	Check_Type(version, T_FIXNUM);
	Check_Type(max_pools, T_FIXNUM);
	Check_Type(expiry, T_FIXNUM);
	Check_Type(cache_size, T_FIXNUM);

	int r = nvm_kv_open(FIX2INT(id), FIX2INT(version), FIX2INT(max_pools), FIX2INT(expiry), NUM2LONG(cache_size));
	return INT2FIX(r);
}

_EXTERN_C_ VALUE wrap_kv_pool_create(VALUE self, VALUE kv_id, VALUE pool_tag) {
	Check_Type(kv_id, T_FIXNUM);
	Check_Type(pool_tag, T_STRING);

	// FIXME: length check
	return INT2FIX(nvm_kv_pool_create(FIX2INT(kv_id),
			(nvm_kv_pool_tag_t*)RSTRING_PTR(pool_tag)));
}

_EXTERN_C_ VALUE wrap_kv_delete_all(VALUE self, VALUE kv_id) {
	Check_Type(kv_id, T_FIXNUM);

	return INT2FIX(nvm_kv_delete_all(FIX2INT(kv_id)));
}

_EXTERN_C_ VALUE wrap_kv_close(VALUE self, VALUE kv_id) {
	Check_Type(kv_id, T_FIXNUM);

	return INT2FIX(nvm_kv_close(FIX2INT(kv_id)));
}

struct nogvl_kv_put_args {
	int kv_id;
	int pool_id;
	nvm_kv_key_t *key;
	int key_len;
	void *value;
	int value_len;
	int expiry;
	int replace;
	int gen_count;
};

int nogvl_kv_put(struct nogvl_kv_put_args *args) {
	return nvm_kv_put(args->kv_id, args->pool_id, args->key,
			args->key_len,
			args->value, args->value_len,
			args->expiry,
			args->replace,
			args->gen_count);
};

_EXTERN_C_ VALUE wrap_kv_put(VALUE self, VALUE kv_id, VALUE pool_id, VALUE key, VALUE value,
		VALUE expiry, VALUE replace, VALUE gen_count) {
	struct nogvl_kv_put_args args;
	int r;

	Check_Type(kv_id, T_FIXNUM);
	Check_Type(pool_id, T_FIXNUM);
	Check_Type(key, T_STRING);
	Check_Type(value, T_STRING);
	Check_Type(expiry, T_FIXNUM);
	Check_Type(replace, T_FIXNUM);
	Check_Type(gen_count, T_FIXNUM);

	char *key_buf = RSTRING_PTR(key);
	int key_len = RSTRING_LEN(key);
	char *value_buf = RSTRING_PTR(value);
	int value_len = RSTRING_LEN(value);

	args.kv_id = FIX2INT(kv_id);
	args.pool_id = FIX2INT(pool_id);
	args.key = (nvm_kv_key_t*) key_buf;
	args.key_len = key_len;
	args.value = (void*)value_buf;
	args.value_len = value_len;
	args.expiry = FIX2INT(expiry);
	args.replace = FIX2INT(replace) != 0;
	args.gen_count = FIX2INT(gen_count);
#ifdef HAVE_RB_THREAD_BLOCKING_REGION
	r = rb_thread_blocking_region(
			(rb_blocking_function_t*) nogvl_kv_put,
			&args, NULL, NULL); 
#else
	r = nogvl_kv_put(&args);
#endif
	return INT2FIX(r);
}

struct nogvl_kv_basic_args {
	int kv_id;
	int pool_id;
	nvm_kv_key_t *key;
	int key_len;
	void *value;
	int value_len;
	bool read_exact;
	nvm_kv_key_info_t *key_info;
};

int nogvl_kv_get(struct nogvl_kv_basic_args *args) {
	return nvm_kv_get(args->kv_id, args->pool_id,
			args->key, args->key_len,
			args->value, args->value_len,
			args->read_exact, args->key_info);
}
	
_EXTERN_C_ VALUE wrap_kv_get(VALUE self, VALUE kv_id, VALUE pool_id, VALUE key) {
	struct nogvl_kv_basic_args args;
	nvm_kv_key_info_t ki;
	char *value_buf = NULL;
	int r;

	Check_Type(kv_id, T_FIXNUM);
	Check_Type(pool_id, T_FIXNUM);
	Check_Type(key, T_STRING);


	char *key_buf = RSTRING_PTR(key);
	int key_len = RSTRING_LEN(key);

	int value_buf_len = nvm_kv_get_val_len(FIX2INT(kv_id), FIX2INT(pool_id), (nvm_kv_key_t*)key_buf, key_len);
	if (value_buf_len < 512) {
		value_buf_len = 512;
	}	
	if (posix_memalign((void**)&value_buf, 4096, value_buf_len)) {
		rb_raise(rb_eNoMemError, "memory allocation failed");
	}

	args.kv_id = FIX2INT(kv_id);
	args.pool_id = FIX2INT(pool_id);
	args.key = (nvm_kv_key_t*) key_buf;
	args.key_len = key_len;
	args.value = value_buf;
	args.value_len = value_buf_len;
	args.read_exact = true;
	args.key_info = &ki;
#ifdef HAVE_RB_THREAD_BLOCKING_REGION
	r = rb_thread_blocking_region((rb_blocking_function_t*) nogvl_kv_get, &args, NULL, NULL);
#else
	r = nogvl_kv_get(&args);
#endif

	VALUE ret = ki_to_hash(&ki);
	rb_hash_aset(ret, rb_str_new2("value"), r < 0 ? rb_str_new("error", 5) : rb_str_new(value_buf, r));
	free(value_buf);
	return ret;
}

struct nogvl_kv_get_args {
	int kv_id;
	int pool_id;
	nvm_kv_key_t *key;
	int key_len;
};

int nogvl_kv_delete(struct nogvl_kv_basic_args *args) {
	return nvm_kv_delete(args->kv_id, args->pool_id,
			args->key, args->key_len);
}
_EXTERN_C_ VALUE wrap_kv_delete(VALUE self, VALUE kv_id, VALUE pool_id,
		VALUE key) {
	struct nogvl_kv_basic_args args;
	int r;

	Check_Type(kv_id, T_FIXNUM);
	Check_Type(pool_id, T_FIXNUM);
	Check_Type(key, T_STRING);

	char *key_buf = RSTRING_PTR(key);
	int key_len = RSTRING_LEN(key);

	args.kv_id = FIX2INT(kv_id);
	args.pool_id = FIX2INT(pool_id);
	args.key = (nvm_kv_key_t*) key_buf;
	args.key_len = key_len;
	
#ifdef HAVE_RB_THREAD_BLOCKING_REGION
	r = rb_thread_blocking_region((rb_blocking_function_t*) nogvl_kv_delete, &args, NULL, NULL);
#else
	r = nogvl_kv_delete(&args);
#endif
	return INT2FIX(r);
}

_EXTERN_C_ VALUE wrap_kv_begin(VALUE self, VALUE kv_id, VALUE pool_id) {
	Check_Type(kv_id, T_FIXNUM);
	Check_Type(pool_id, T_FIXNUM);
	return INT2FIX(nvm_kv_begin(FIX2INT(kv_id), FIX2INT(pool_id)));
}

_EXTERN_C_ VALUE wrap_kv_next(VALUE self, VALUE kv_id, VALUE iterator_id) {
	Check_Type(kv_id, T_FIXNUM);
	Check_Type(iterator_id, T_FIXNUM);

	return INT2FIX(nvm_kv_next(FIX2INT(kv_id),
			FIX2INT(iterator_id)));
}

_EXTERN_C_ VALUE wrap_kv_get_current(VALUE self, VALUE kv_id, VALUE iterator_id) {
	int value_buf_len = NVM_KV_MAX_VALUE_SIZE;
	char key_buf[NVM_KV_MAX_KEY_SIZE+1];
	uint32_t key_len = NVM_KV_MAX_KEY_SIZE;
	char* value_buf;
	nvm_kv_key_info_t ki;
	int actual_length;

	Check_Type(kv_id, T_FIXNUM);
	Check_Type(iterator_id, T_FIXNUM);

	// FIXME: 
	VALUE r = Qnil;

	if (posix_memalign((void**)&value_buf, 4096, value_buf_len)) {
		rb_raise(rb_eNoMemError, "memory allocation failed");
	}

	actual_length = nvm_kv_get_current(FIX2INT(kv_id), FIX2INT(iterator_id),
			(nvm_kv_key_t*)&key_buf, &key_len,
			value_buf, value_buf_len, &ki);

	if (actual_length > value_buf_len) {
		free(value_buf);
		value_buf_len = actual_length;
		if (posix_memalign((void**)&value_buf, 4096, value_buf_len)) {
			rb_raise(rb_eNoMemError, "memory allocation failed");
		}

		actual_length = nvm_kv_get_current(FIX2INT(kv_id), FIX2INT(iterator_id),
				(nvm_kv_key_t*)&key_buf, &key_len,
				value_buf, value_buf_len, &ki);
	}

	if (actual_length == -1) {
		// Failed
		r = Qnil;
	} else if (actual_length >= 0) {
		r = ki_to_hash(&ki);	
		rb_hash_aset(r, rb_str_new2("key"), rb_str_new(key_buf, key_len));
		rb_hash_aset(r, rb_str_new2("value"), rb_str_new(value_buf, actual_length));
	}

	free(value_buf);

	return (r);
}

_EXTERN_C_ VALUE wrap_kv_iteration_end(VALUE self, VALUE kv_id, VALUE iterator_id) {
	Check_Type(kv_id, T_FIXNUM);
	Check_Type(iterator_id, T_FIXNUM);

	return INT2FIX(nvm_kv_iteration_end(
			FIX2INT(kv_id), FIX2INT(iterator_id)));
}


int nogvl_kv_exists(struct nogvl_kv_basic_args *args) {
	return nvm_kv_exists(args->kv_id, args->pool_id,
			args->key, args->key_len, args->key_info);
}

_EXTERN_C_ VALUE wrap_kv_exists(VALUE self, VALUE kv_id, VALUE pool_id,
		VALUE key) {
	nvm_kv_key_info_t ki;
	int r;
	struct nogvl_kv_basic_args args;

	Check_Type(kv_id, T_FIXNUM);
	Check_Type(pool_id, T_FIXNUM);
	Check_Type(key, T_STRING);

	char *key_buf = RSTRING_PTR(key);
	int key_len = RSTRING_LEN(key);

	args.kv_id = FIX2INT(kv_id);
	args.pool_id = FIX2INT(pool_id);
	args.key = (nvm_kv_key_t*) key_buf;
	args.key_len = key_len;
	args.key_info = &ki;

#ifdef HAVE_RB_THREAD_BLOCKING_REGION
	r = rb_thread_blocking_region((rb_blocking_function_t*) nogvl_kv_exists, &args, NULL, NULL);
#else
	r = nogvl_kv_exists(&args);
#endif
	// FIXME: return KI
	return INT2FIX(r);
}

struct nogvl_kv_batch_put_args {
	int kv_id;
	int pool_id;
	nvm_kv_iovec_t *iov;
	int iov_count;
};

int nogvl_kv_batch_put(struct nogvl_kv_batch_put_args *args) {
	int r = nvm_kv_batch_put(args->kv_id, args->pool_id, args->iov, args->iov_count);
	return r;
}

_EXTERN_C_ VALUE wrap_kv_batch_put(VALUE self, VALUE kv_id, VALUE pool_id, VALUE list) {
	Check_Type(kv_id, T_FIXNUM);
	Check_Type(pool_id, T_FIXNUM);
	Check_Type(list, T_ARRAY);
 	// We trust and don't check entities in the list
 	// since it is generated by NVMKV internally.  
 
	int batch_max = 64;
	nvm_kv_iovec_t iov[64];
	char key_buf[64][128 + 1];

	int entries = RARRAY_LEN(list);
	int i, n;
	int result = -1;

	struct nogvl_kv_batch_put_args args;
	args.kv_id = FIX2INT(kv_id);
	args.pool_id = FIX2INT(pool_id);

	i = 0;
	for (n = 0; n < entries; n++) {
		VALUE entry = RARRAY_PTR(list)[n];

		// entry = [key, value, expiry, gen_count, replace]
		//
		if (RARRAY_LEN(entry) < 5 ||
				TYPE(RARRAY_PTR(entry)[0]) != T_STRING ||
				TYPE(RARRAY_PTR(entry)[1]) != T_STRING ||
				TYPE(RARRAY_PTR(entry)[2]) != T_FIXNUM ||
				TYPE(RARRAY_PTR(entry)[3]) != T_FIXNUM ||
				TYPE(RARRAY_PTR(entry)[4]) != T_FIXNUM ) {
			rb_raise(rb_eTypeError, "invalid batch entry"); 
		}	

		iov[i].key = (nvm_kv_key_t*) &key_buf[i];
		iov[i].key_len = RSTRING_LEN(RARRAY_PTR(entry)[0]);
		strncpy((char*) &key_buf[i], RSTRING_PTR(RARRAY_PTR(entry)[0]), 128);

		iov[i].value_len = RSTRING_LEN(RARRAY_PTR(entry)[1]); 
		if (posix_memalign((void**)&iov[i].value, 4096,
				iov[i].value_len + 1)) {
			rb_raise(rb_eNoMemError, "memory allocation failed");
		}
		strncpy((char*)iov[i].value, (char*)RSTRING_PTR(RARRAY_PTR(entry)[1]), iov[i].value_len + 1);

		iov[i].expiry = FIX2INT(RARRAY_PTR(entry)[2]);
		iov[i].gen_count = FIX2INT(RARRAY_PTR(entry)[3]);
		iov[i].replace = FIX2INT(RARRAY_PTR(entry)[4]);
		iov[i].reserved1 = FIX2INT(RARRAY_PTR(entry)[5]);

#if 0
		printf("iov[%d]: key %s key_len %d value %s value_len %d expiry %d gen_count %d replace %d reserved1 %d\n",
				i, iov[i].key, iov[i].key_len, (char*)iov[i].value, iov[i].value_len,
				iov[i].expiry, iov[i].gen_count, iov[i].replace, iov[i].reserved1);
#endif

		i++;

		if ((n + 1 >= entries) || i >= batch_max) {
			args.iov = (nvm_kv_iovec_t*) &iov;
			args.iov_count = i;

#ifdef HAVE_RB_THREAD_BLOCKING_REGION
			result = rb_thread_blocking_region((rb_blocking_function_t*) nogvl_kv_batch_put, &args, NULL, NULL);
#else
			result = nogvl_kv_batch_put(&args);
#endif
			if (result != 0) {
				break;
			}
			while (i > 0) {
				i--;
				free(iov[i].value);
			}
		}
	} // for (n)

	while (i > 0) {
		i--;
		free(iov[i].value);
	}

	return INT2FIX(result);
}

_EXTERN_C_ VALUE wrap_kv_pool_delete(VALUE self, VALUE kv_id, VALUE pool_id) {
	Check_Type(kv_id, T_FIXNUM);
	Check_Type(pool_id, T_FIXNUM);

	return nvm_kv_pool_delete(FIX2INT(kv_id), FIX2INT(pool_id));
}

_EXTERN_C_ VALUE wrap_kv_get_pool_info(VALUE self, VALUE kv_id, VALUE pool_id) {
	nvm_kv_pool_info_t pi;
	int r;

	Check_Type(kv_id, T_FIXNUM);
	Check_Type(pool_id, T_FIXNUM);

	r = nvm_kv_get_pool_info(FIX2INT(kv_id), FIX2INT(pool_id), &pi);

	VALUE ret = rb_hash_new();
	rb_hash_aset(ret, rb_str_new2("version"), INT2FIX(pi.version));
	rb_hash_aset(ret, rb_str_new2("pool_status"), INT2FIX(pi.pool_status));
	return ret;
}

_EXTERN_C_ VALUE wrap_kv_get_store_info(VALUE self, VALUE kv_id) {
	Check_Type(kv_id, T_FIXNUM);

	VALUE ret;
	nvm_kv_store_info_t si;

	int r = nvm_kv_get_store_info(FIX2INT(kv_id), &si);
	if (r < 0) {
		ret = Qnil;
	} else {
		ret = rb_hash_new();
		rb_hash_aset(ret, rb_str_new2("version"), INT2FIX(si.version));
		rb_hash_aset(ret, rb_str_new2("num_pools"), INT2FIX(si.num_pools));
		rb_hash_aset(ret, rb_str_new2("max_pools"), INT2FIX(si.max_pools));
		rb_hash_aset(ret, rb_str_new2("expiry_mode"), INT2FIX(si.expiry_mode));
		rb_hash_aset(ret, rb_str_new2("global_expiry"), INT2FIX(si.global_expiry));
		rb_hash_aset(ret, rb_str_new2("cache_size"), LONG2NUM(si.cache_size));
		rb_hash_aset(ret, rb_str_new2("num_keys"), LONG2NUM(si.num_keys));
		rb_hash_aset(ret, rb_str_new2("free_space"), LONG2NUM(si.free_space));
	}
	return ret;
}

_EXTERN_C_ VALUE wrap_kv_get_key_info(VALUE self, VALUE kv_id,
		VALUE pool_id, VALUE key) {
	VALUE ret;
	nvm_kv_key_info_t ki;

	Check_Type(kv_id, T_FIXNUM);
	Check_Type(pool_id, T_FIXNUM);
	Check_Type(key, T_STRING);

	char *key_buf = RSTRING_PTR(key);
	int key_len = RSTRING_LEN(key);

	int r = nvm_kv_get_key_info(FIX2INT(kv_id), FIX2INT(pool_id),
			(nvm_kv_key_t*) key_buf, key_len, &ki);
	if (r < 0) {
		ret = Qnil;
	} else {
		ret = ki_to_hash(&ki);	
	}

	return ret;
}

_EXTERN_C_ VALUE wrap_kv_set_global_expiry(VALUE self, VALUE kv_id,
		VALUE expiry) {

	Check_Type(kv_id, T_FIXNUM);
	Check_Type(expiry, T_FIXNUM);

	int r = nvm_kv_set_global_expiry(FIX2INT(kv_id), FIX2INT(expiry));
	return INT2FIX(r); 
}

//_EXTERN_C_ VALUE wrap_kv_get_pool_metadata(VALUE self, VALUE kv_id,
//		VALUE 

_EXTERN_C_ void Init_nvmkv_driver()
{
	VALUE module;

	module = rb_define_module("Nvmkv");
	rb_define_module_function(module, "file_open", _RB_WRAP_FUNC_(wrap_syscall_open), 1);

	rb_define_module_function(module, "kv_open", _RB_WRAP_FUNC_(wrap_kv_open), 5);
	rb_define_module_function(module, "kv_pool_create", _RB_WRAP_FUNC_(wrap_kv_pool_create), 2);
	rb_define_module_function(module, "kv_delete_all", _RB_WRAP_FUNC_(wrap_kv_delete_all), 1);
	rb_define_module_function(module, "kv_close", _RB_WRAP_FUNC_(wrap_kv_close), 1);
	rb_define_module_function(module, "kv_put", _RB_WRAP_FUNC_(wrap_kv_put), 7);
	rb_define_module_function(module, "kv_get", _RB_WRAP_FUNC_(wrap_kv_get), 3);
	rb_define_module_function(module, "kv_delete", _RB_WRAP_FUNC_(wrap_kv_delete), 3);
	rb_define_module_function(module, "kv_begin", _RB_WRAP_FUNC_(wrap_kv_begin), 2);
	rb_define_module_function(module, "kv_next", _RB_WRAP_FUNC_(wrap_kv_next), 2);
	rb_define_module_function(module, "kv_get_current", _RB_WRAP_FUNC_(wrap_kv_get_current), 2);
	rb_define_module_function(module, "kv_iteration_end", _RB_WRAP_FUNC_(wrap_kv_iteration_end), 2);
	rb_define_module_function(module, "kv_exists", _RB_WRAP_FUNC_(wrap_kv_exists), 3);
	rb_define_module_function(module, "kv_batch_put", _RB_WRAP_FUNC_(wrap_kv_batch_put), 3);
	rb_define_module_function(module, "kv_pool_delete", _RB_WRAP_FUNC_(wrap_kv_pool_delete), 2);
	rb_define_module_function(module, "kv_get_pool_info", _RB_WRAP_FUNC_(wrap_kv_get_pool_info), 2);
	rb_define_module_function(module, "kv_get_store_info", _RB_WRAP_FUNC_(wrap_kv_get_store_info), 1);
	rb_define_module_function(module, "kv_get_key_info", _RB_WRAP_FUNC_(wrap_kv_get_key_info), 3);
	rb_define_module_function(module, "kv_set_global_expiry", _RB_WRAP_FUNC_(wrap_kv_set_global_expiry), 2);

	rb_define_const(module, "MAX_POOLS", INT2FIX(NVM_KV_MAX_POOLS));
	rb_define_const(module, "MAX_KEY_SIZE", INT2FIX(NVM_KV_MAX_KEY_SIZE));
	rb_define_const(module, "MAX_VALUE_SIZE", INT2FIX(NVM_KV_MAX_VALUE_SIZE));
	rb_define_const(module, "MAX_ITERATORS", INT2FIX(NVM_KV_MAX_ITERATORS));

	rb_define_const(module, "KV_DISABLE_EXPIRY", INT2FIX(KV_DISABLE_EXPIRY));
	rb_define_const(module, "KV_ARBITRARY_EXPIRY", INT2FIX(KV_ARBITRARY_EXPIRY));
	rb_define_const(module, "KV_GLOBAL_EXPIRY", INT2FIX(KV_GLOBAL_EXPIRY));
}
