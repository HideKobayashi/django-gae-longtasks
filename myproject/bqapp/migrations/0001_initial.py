# Generated by Django 4.1 on 2022-08-20 12:53

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='MainTask',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('main_job_id', models.CharField(max_length=255, unique=True, verbose_name='メインジョブID')),
                ('main_task_name', models.CharField(max_length=255, verbose_name='メインタスク名')),
                ('process_state', models.CharField(choices=[('0', '未処理'), ('1', '処理済'), ('2', '処理中'), ('3', '失敗')], max_length=255, verbose_name='処理状態')),
                ('process_start_time', models.DateTimeField(verbose_name='処理開始時刻')),
                ('in_data', models.JSONField(verbose_name='入力データ')),
                ('out_data', models.JSONField(verbose_name='出力データ')),
                ('created_at', models.DateTimeField(auto_now_add=True, verbose_name='登録日時')),
                ('updated_at', models.DateTimeField(auto_now=True, verbose_name='更新日時')),
            ],
            options={
                'db_table': 'main_task',
            },
        ),
        migrations.CreateModel(
            name='SubTask',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('job_id', models.CharField(max_length=255, unique=True, verbose_name='ジョブID')),
                ('sub_task_name', models.CharField(max_length=255, verbose_name='サブタスク名')),
                ('process_state', models.CharField(choices=[('0', '未処理'), ('1', '処理済'), ('2', '処理中'), ('3', '失敗')], max_length=255, verbose_name='処理状態')),
                ('process_start_time', models.DateTimeField(verbose_name='処理開始時刻')),
                ('in_data', models.JSONField(verbose_name='入力データ')),
                ('out_data', models.JSONField(verbose_name='出力データ')),
                ('created_at', models.DateTimeField(auto_now_add=True, verbose_name='登録日時')),
                ('updated_at', models.DateTimeField(auto_now=True, verbose_name='更新日時')),
                ('main_job_id', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='bqapp.maintask', verbose_name='')),
            ],
            options={
                'db_table': 'sub_task',
            },
        ),
    ]