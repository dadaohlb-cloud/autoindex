# feature/stats.py
列统计服务
# frature/index_feat.py
i_type_btree / i_type_fiting
用 one-hot 的方式告诉模型：这是哪类索引。
i_col_count
索引列数。单列和复合索引的效果、空间、维护代价通常不同。
i_is_single / i_is_multi
是 i_col_count 的补充离散表达，能帮助模型更容易学到“单列索引”和“多列索引”是两种不同情况。
i_key_pos_mean
这是一个轻量的“键列顺序编码”。用平均位置值表达。
例如 (a,b,c) 的位置是 1,2,3，均值为 2.0。
i_is_covering
判断这个索引是否覆盖查询所需列。
这是很有价值的特征，因为覆盖索引往往能减少回表。
i_row_count
表总行数。
i_ndv_mean
索引列平均不同值数。
能反映列的基数高低。
i_ndv_ratio_mean
平均不同值比例。
比绝对 NDV 更稳，因为它消除了表规模影响。
i_null_ratio_mean
平均空值比例。
空值比例高的列，索引收益往往会受影响。
i_range_span_mean
平均取值范围跨度。
主要对数值列有效。
i_iqr_span_mean
平均四分位距。
相比 range，它对极端值更不敏感，更稳。
i_numeric_col_ratio
索引列里数值型列所占比例。
这个特征对 FITing 很有用，因为 learned index 更适合数值型有序键。
"i_storage_est_btree"
"i_storage_est_fiting"
"i_storage_est"
当前候选索引类型真正要使用的空间估计值。
如果当前候选是 btree，就取 i_storage_est_btree
如果当前候选是 fiting，就取 i_storage_est_fiting
