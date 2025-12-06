#include "DHT.h"

// --- Definisi Sensor Pertama (DHT1) ---
#define DHTPIN_1 4         // Pin data DHT22 pertama dihubungkan ke GPIO4
#define DHTTYPE_1 DHT22    // Tipe sensor DHT22
DHT dht1(DHTPIN_1, DHTTYPE_1);

// --- Definisi Sensor Kedua (DHT2) ---
#define DHTPIN_2 5         // Pin data DHT22 kedua dihubungkan ke GPIO5 (Ganti dengan pin yang Anda gunakan)
#define DHTTYPE_2 DHT22
DHT dht2(DHTPIN_2, DHTTYPE_2);

void setup() {
  Serial.begin(115200);
  Serial.println("Inisialisasi Sensor DHT22 Ganda...");
  
  // Inisialisasi kedua sensor
  dht1.begin();
  dht2.begin();
}

void loop() {
  // Tunggu sebentar antar pembacaan (DHT22 butuh waktu)
  delay(1000);

  // ------------------------------------
  // --- Pembacaan Sensor Pertama (DHT1) ---
  // ------------------------------------
  float humidity1 = dht1.readHumidity();
  float temperature1 = dht1.readTemperature();  // default Celsius

  // Periksa apakah pembacaan DHT1 gagal
  if (isnan(humidity1) || isnan(temperature1)) {
    Serial.println("Gagal membaca data dari DHT1!");
  } else {
    // Tampilkan data DHT1
    Serial.println("--- Data Sensor 1 ---");
    Serial.print("Suhu: ");
    Serial.print(temperature1);
    Serial.print(" °C  |  Kelembapan: ");
    Serial.print(humidity1);
    Serial.println(" %");
  }

  // ------------------------------------
  // --- Pembacaan Sensor Kedua (DHT2) ---
  // ------------------------------------
  float humidity2 = dht2.readHumidity();
  float temperature2 = dht2.readTemperature();  // default Celsius

  // Periksa apakah pembacaan DHT2 gagal
  if (isnan(humidity2) || isnan(temperature2)) {
    Serial.println("Gagal membaca data dari DHT2!");
  } else {
    // Tampilkan data DHT2
    Serial.println("--- Data Sensor 2 ---");
    Serial.print("Suhu: ");
    Serial.print(temperature2);
    Serial.print(" °C  |  Kelembapan: ");
    Serial.print(humidity2);
    Serial.println(" %");
  }

  Serial.println("-------------------------"); // Pemisah untuk pembacaan berikutnya
}