#define MAXLEVEL    32

        return sl_contains(set, (sl_key_t) key);
        return sl_insert(set, (sl_key_t) key, (val_t) ((long)key));
	return sl_delete(set, (sl_key_t) key);

        /* create the skip list set and do inits */
        ptst_subsystem_init();
        gc_subsystem_init();
        set_subsystem_init();
        set = set_new(1);
	stop = 0;

        // nullify all the index nodes we created so
        // we can start again and rebalance the skip list
        bg_stop();

        // the following code is hacky since it creates a memory
        // leak - we cut off all the nodes in the index levels
        // without reclaiming them - this is only a one-off though
        ptst = ptst_critical_enter();
        set->top = inode_new(NULL, NULL, set->head, ptst);
        ptst_critical_exit(ptst);
        set->head->level = 1;
        temp = set->head->next;
        while (temp) {
            temp->level = 0;
            temp = temp->next;
        }

        // wait till the list is balanced
        bg_start(0);
        while (set->head->level < floor_log_2(initial)) {
            AO_nop_full();
        }
        printf("Number of levels is %d\n", set->head->level);
        bg_stop();
        bg_start(1000000);

