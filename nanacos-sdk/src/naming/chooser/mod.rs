use crate::api::error::Error::ErrResult;
use crate::api::error::Result;
use crate::api::naming::{InstanceChooser, ServiceInstance};
use rand::RngExt;

pub(crate) struct RandomWeightChooser {
    /// 与实例下标对应的累计概率边界。
    weights: Vec<f64>,

    /// 跳过非正权重后保留的候选实例。
    items: Vec<ServiceInstance>,
}

impl RandomWeightChooser {
    /// 业务作用：从候选实例构建累计权重表，为单次随机选择准备概率区间。
    ///
    /// 参数说明：
    /// - `service_name`：空候选错误中使用的服务标识。
    /// - `items`：上层已筛选的实例；当前入口跳过非正权重，但不额外检查健康或启用状态。
    ///
    /// 返回：累计概率在容差内时返回选择器；输入为空或累计概率不合法时返回错误。全为非正权重会返回空选择器，非有限权重不保证能构造成功。
    pub fn new(service_name: String, items: Vec<ServiceInstance>) -> Result<Self> {
        if items.is_empty() {
            return Err(ErrResult(format!(
                "no available {service_name} service instance can be selected"
            )));
        }
        let mut init_items: Vec<ServiceInstance> = Vec::with_capacity(items.len());
        let mut origin_weight_sum = 0_f64;

        let mut count = 0;
        for instance in items.iter() {
            let mut weight = instance.weight;

            if weight <= 0_f64 {
                // 非正权重不参与调度，不能把仅因仍在目录中的实例作为可选目标。
                continue;
            }

            if weight.is_infinite() {
                weight = 10000.0_f64;
            }

            if weight.is_nan() {
                weight = 1.0_f64;
            }
            origin_weight_sum += weight;
            count += 1;
        }

        let mut exact_weights: Vec<f64> = Vec::with_capacity(count);
        let mut index = 0;

        for instance in items.into_iter() {
            let single_weight = instance.weight;

            if single_weight <= 0_f64 {
                continue;
            }

            init_items.push(instance);

            exact_weights.insert(index, single_weight / origin_weight_sum);
            index += 1;
        }

        let mut weights: Vec<f64> = Vec::with_capacity(count);

        let mut random_range = 0_f64;

        for (i, exact_weights_item) in exact_weights.iter().enumerate().take(index) {
            weights.insert(i, random_range + exact_weights_item);
            random_range += exact_weights_item;
        }

        let double_precision_delta = 0.0001_f64;

        if index == 0 || (weights[index - 1] - 1.0_f64).abs() < double_precision_delta {
            return Ok(RandomWeightChooser {
                weights,
                items: init_items,
            });
        }

        Err(ErrResult(
            "Cumulative Weight calculate wrong , the sum of probabilities does not equals 1."
                .to_string(),
        ))
    }
}

impl InstanceChooser for RandomWeightChooser {
    /// 业务作用：按累计权重抽取一个服务实例，消费本次选择器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：命中区间对应实例的副本；未能定位区间时回退到最后实例，候选为空则返回空，不建立网络连接。
    fn choose(mut self) -> Option<ServiceInstance> {
        let random_number = rand::rng().random_range(0.0..1.0);
        let index = self.weights.binary_search_by(|d| {
            d.partial_cmp(&random_number)
                .unwrap_or(std::cmp::Ordering::Less)
        });
        if let Ok(index) = index {
            let instance = self.items.get(index);
            if let Some(instance) = instance {
                return Some(instance.to_owned());
            } else {
                return self.items.pop();
            }
        } else {
            let index = index.unwrap_err();
            if index < self.weights.len() {
                let Some(weight) = self.weights.get(index) else {
                    return self.items.pop();
                };
                if random_number < *weight {
                    let Some(instance) = self.items.get(index) else {
                        return self.items.pop();
                    };
                    return Some(instance.to_owned());
                }
            }
        }

        self.items.pop()
    }
}
