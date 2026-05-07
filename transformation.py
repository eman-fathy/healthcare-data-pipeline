# =============================================================================
# Healthcare Data Pipeline - مع حل مشكلة أسماء الأعمدة
# =============================================================================

# ─────────────────────────────────────────────────────────────────────────────
# CELL 1 — إعدادات الاتصال
# ─────────────────────────────────────────────────────────────────────────────

spark.conf.set(
    "YOUR_ACCESS_KEY_HERE"
)

raw_path = "abfss://healthcare-data@healthcaredatalake01.dfs.core.windows.net/raw/"
silver_path = "abfss://healthcare-data@healthcaredatalake01.dfs.core.windows.net/silver/"

print("✅ تم إعداد الاتصال")

# ─────────────────────────────────────────────────────────────────────────────
# CELL 2 — قراءة البيانات
# ─────────────────────────────────────────────────────────────────────────────

df_health = spark.read.csv(
    f"{raw_path}healthcare_dataset.csv",
    header=True,
    inferSchema=True
)

df_disease = spark.read.csv(
    f"{raw_path}api_disease_data.csv",
    header=True,
    inferSchema=True
)

print(f"✅ تم تحميل {df_health.count()} سجل من بيانات المرضى")
print(f"✅ تم تحميل {df_disease.count()} سجل من بيانات الأمراض")

# عرض أسماء الأعمدة الأصلية
print("\n📋 أسماء الأعمدة الأصلية:")
print(df_health.columns)

# ─────────────────────────────────────────────────────────────────────────────
# CELL 3 — إعادة تسمية الأعمدة (حل مشكلة Delta)
# ─────────────────────────────────────────────────────────────────────────────

# قاموس تحويل الأسماء
rename_dict = {
    "Blood Type": "blood_type",
    "Medical Condition": "medical_condition",
    "Date of Admission": "admission_date",
    "Insurance Provider": "insurance_provider",
    "Billing Amount": "billing_amount",
    "Room Number": "room_number",
    "Admission Type": "admission_type",
    "Discharge Date": "discharge_date",
    "Test Results": "test_results",
    "Name": "patient_name",
    "Doctor": "doctor_name",
    "Hospital": "hospital_name",
    "Medication": "medication_name"
}

# تطبيق إعادة التسمية
for old_name, new_name in rename_dict.items():
    if old_name in df_health.columns:
        df_health = df_health.withColumnRenamed(old_name, new_name)
        print(f"✓ {old_name} → {new_name}")

print("\n✅ تم إعادة تسمية جميع الأعمدة")

# ─────────────────────────────────────────────────────────────────────────────
# CELL 4 — تنظيف البيانات
# ─────────────────────────────────────────────────────────────────────────────

from pyspark.sql import functions as F

# تنظيف الأسماء
df_cleaned = df_health \
    .withColumn("patient_name", F.initcap(F.trim(F.col("patient_name")))) \
    .withColumn("doctor_name", F.initcap(F.trim(F.col("doctor_name")))) \
    .withColumn("hospital_name", F.initcap(F.trim(F.col("hospital_name"))))

# حذف المكررات
rows_before = df_cleaned.count()
df_cleaned = df_cleaned.dropDuplicates()
print(f"🗑️ تم حذف {rows_before - df_cleaned.count()} سجل مكرر")

# معالجة القيم المفقودة
critical_cols = ["patient_name", "Age", "Gender", "admission_date", "discharge_date"]
df_cleaned = df_cleaned.dropna(subset=critical_cols)

# تعبئة القيم المفقودة
fill_values = {
    "medical_condition": "Unknown",
    "insurance_provider": "Not Specified",
    "medication_name": "Not Prescribed",
    "test_results": "Pending",
    "blood_type": "Unknown",
    "admission_type": "Standard"
}
df_cleaned = df_cleaned.fillna(fill_values)

# تصفية الأعمار غير المنطقية
df_cleaned = df_cleaned.filter((F.col("Age") >= 0) & (F.col("Age") <= 120))

print(f"✅ بعد التنظيف: {df_cleaned.count()} سجل")

# ─────────────────────────────────────────────────────────────────────────────
# CELL 5 — معالجة التواريخ
# ─────────────────────────────────────────────────────────────────────────────

# تحويل التواريخ (جرب صيغ مختلفة)
date_formats = ["yyyy-MM-dd", "MM/dd/yyyy", "dd/MM/yyyy"]

for date_format in date_formats:
    temp_df = df_cleaned.withColumn(
        "admission_date_temp",
        F.to_date(F.col("admission_date"), date_format)
    )
    if temp_df.filter(F.col("admission_date_temp").isNotNull()).count() > 0:
        df_cleaned = df_cleaned.withColumn("admission_date", F.to_date(F.col("admission_date"), date_format))
        df_cleaned = df_cleaned.withColumn("discharge_date", F.to_date(F.col("discharge_date"), date_format))
        print(f"✅ تم تحويل التواريخ باستخدام الصيغة: {date_format}")
        break

# حذف التواريخ غير الصالحة
df_cleaned = df_cleaned.filter(
    F.col("admission_date").isNotNull() & 
    F.col("discharge_date").isNotNull()
)

# ─────────────────────────────────────────────────────────────────────────────
# CELL 6 — إضافة أعمدة محسوبة
# ─────────────────────────────────────────────────────────────────────────────

# مدة الإقامة
df_cleaned = df_cleaned.withColumn(
    "length_of_stay",
    F.datediff(F.col("discharge_date"), F.col("admission_date"))
)
df_cleaned = df_cleaned.filter(F.col("length_of_stay") >= 0)

# الفئة العمرية
df_cleaned = df_cleaned.withColumn(
    "age_group",
    F.when(F.col("Age") < 18, "Child")
     .when(F.col("Age") < 35, "Young_Adult")
     .when(F.col("Age") < 50, "Adult")
     .when(F.col("Age") < 65, "Middle_Age")
     .otherwise("Senior")
)

# موسم الدخول
df_cleaned = df_cleaned.withColumn(
    "admission_season",
    F.when(F.month("admission_date").isin(12, 1, 2), "Winter")
     .when(F.month("admission_date").isin(3, 4, 5), "Spring")
     .when(F.month("admission_date").isin(6, 7, 8), "Summer")
     .otherwise("Fall")
)

print("✅ تم إضافة الأعمدة المحسوبة")

# ─────────────────────────────────────────────────────────────────────────────
# CELL 7 — التحقق من أسماء الأعمدة النهائية
# ─────────────────────────────────────────────────────────────────────────────

print("\n📋 أسماء الأعمدة النهائية (مناسبة لـ Delta):")
for col in df_cleaned.columns:
    print(f"   ✓ {col}")

# التأكد من عدم وجود أحرف غير مسموحة
invalid_chars = [' ', ',', ';', '{', '}', '(', ')', '\n', '\t', '=']
has_invalid = False
for col in df_cleaned.columns:
    for char in invalid_chars:
        if char in col:
            print(f"⚠️ تحذير: العمود '{col}' يحتوي على حرف غير مسموح: '{char}'")
            has_invalid = True

if not has_invalid:
    print("\n✅ جميع أسماء الأعمدة صالحة لـ Delta Lake")

# ─────────────────────────────────────────────────────────────────────────────
# CELL 8 — حفظ البيانات في Delta (الحل النهائي)
# ─────────────────────────────────────────────────────────────────────────────

# حفظ كـ Delta
df_cleaned.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.autoOptimize.optimizeWrite", "true") \
    .partitionBy("age_group") \
    .save(f"{silver_path}healthcare_cleaned")

print(f"\n✅ تم حفظ {df_cleaned.count()} سجل في طبقة Silver")
print(f"📍 المسار: {silver_path}healthcare_cleaned")

# ─────────────────────────────────────────────────────────────────────────────
# CELL 9 — التحقق من القراءة
# ─────────────────────────────────────────────────────────────────────────────

# قراءة للتحقق
df_verify = spark.read.format("delta").load(f"{silver_path}healthcare_cleaned")
print(f"\n✅ تم التحقق: {df_verify.count()} سجل محفوظ")

# عرض عينة
print("\n📊 عينة من البيانات النهائية:")
df_verify.select(
    "patient_name", "Age", "age_group", 
    "medical_condition", "length_of_stay", "admission_date"
).show(10, truncate=False)

print("\n🎉 Pipeline اكتمل بنجاح!")