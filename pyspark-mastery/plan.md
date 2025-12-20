
pyspark-master/
│
├── README.md                      # How to use this repo (learning path)
│
├── 00_setup/
│   ├── architecture/
│   │   ├── spark_architecture.md
│   │   ├── driver_executor.md
│   │   └── cluster_manager.md
│   │
│   ├── installation/
│   │   ├── local_installation.md
│   │   ├── docker_installation.md
│   │   └── troubleshooting.md
│
├── 01_fundamentals/
│   ├── spark_session.ipynb
│   ├── rdd_intro.ipynb
│   ├── dataframe_intro.ipynb
│   ├── lazy_evaluation.md
│   └── transformations_vs_actions.md
│
├── 02_rdd_mastery/               # ⭐ INTERVIEW GOLD
│   ├── map_flatmap_filter.ipynb
│   ├── groupByKey_vs_reduceByKey.ipynb
│   ├── combineByKey.ipynb
│   ├── repartition_vs_coalesce.ipynb
│   ├── wide_vs_narrow_transformations.md
│   ├── shuffle_explained.md
│   └── fault_tolerance.md
│
├── 03_dataframe_mastery/
│   ├── dataframe_basics.ipynb
│   ├── column_expressions.ipynb
│   ├── joins/
│   │   ├── inner_left_right_full.ipynb
│   │   ├── broadcast_join.ipynb
│   │   └── skewed_join_handling.md
│   ├── aggregations.ipynb
│   ├── window_functions.ipynb
│   └── explode_array_map.ipynb
│
├── 04_spark_sql/
│   ├── spark_sql_basics.ipynb
│   ├── temp_vs_global_views.md
│   ├── sql_vs_dataframe.md
│   └── optimization_with_sql.md
│
├── 05_performance_optimization/  # ⭐ MOST ASKED
│   ├── partitioning.md
│   ├── caching_persistence.md
│   ├── join_optimization.md
│   ├── file_formats.md
│   ├── bucketing.md
│   ├── handling_data_skew.md
│   └── tuning_spark_conf.md
│
├── 06_streaming/
│   ├── structured_streaming_intro.ipynb
│   ├── windowed_aggregations.ipynb
│   ├── watermarking.md
│   └── checkpointing.md
│
├── 07_mllib/
│   ├── feature_engineering.ipynb
│   ├── classification.ipynb
│   ├── regression.ipynb
│   ├── clustering.ipynb
│   └── pipelines.ipynb
│
├── 08_real_world_projects/       # ⭐ RESUME MAKER
│   ├── etl_pipeline/
│   │   ├── README.md
│   │   ├── ingest.py
│   │   ├── transform.py
│   │   ├── validate.py
│   │   └── write.py
│   │
│   ├── data_lake/
│   │   ├── bronze_silver_gold.md
│   │   └── implementation.ipynb
│   │
│   ├── analytics_project/
│   │   └── README.md
│   │
│   └── streaming_project/
│       └── README.md
│
├── 09_production_spark/          # ⭐ SENIOR LEVEL
│   ├── project_structure.md
│   ├── logging_monitoring.md
│   ├── error_handling.md
│   ├── deployment_emr.md
│   ├── deployment_databricks.md
│   └── spark_submit.md
│
├── 10_interview_preparation/     # ⭐ FINAL STAGE
│   ├── coding_questions/
│   │   ├── word_count.ipynb
│   │   ├── top_n.ipynb
│   │   ├── running_total.ipynb
│   │   └── sessionization.ipynb
│   │
│   ├── system_design/
│   │   ├── design_large_etl.md
│   │   ├── streaming_pipeline_design.md
│   │   └── data_lake_design.md
│   │
│   ├── common_questions.md
│   ├── optimization_scenarios.md
│   └── mock_interviews.md
│
├── datasets/
│   ├── small/
│   └── large/
│
├── tests/
│   ├── test_rdd.py
│   ├── test_dataframe.py
│   └── test_sql.py
│
└── resources/
    ├── cheat_sheets/
    ├── best_practices.md
    └── troubleshooting.md
