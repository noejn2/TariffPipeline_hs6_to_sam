using TariffPipeline_hs6_to_sam

result = hs6_to_sam_pipeline("CHINA", 2023)

using DataFrames
result["imports"]
result["exports"]