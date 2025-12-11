import os
import json
import traceback
from datetime import date, timedelta
from asyncio import CancelledError
from asyncio import run

from src.application import TikTokDownloader
from src.application.main_terminal import TikTok
from src.tools import to_upload_cookie_list
from src.uploader.upload import upload_video, upload_videos

import argparse
import datetime
import pytz


def get_next_sunday():
    """获取下一个周日的 datetime 对象，时间设置为上午10点，分钟为5的倍数"""
    now = datetime.datetime.now(pytz.UTC)
    today = now.date()

    # 计算到下一个周日需要的天数 (周日是 weekday() == 6)
    days_ahead = 6 - today.weekday()

    # 如果今天就是周日，我们需要下周的周日
    if days_ahead <= 0:
        days_ahead += 7

    next_sunday = today + datetime.timedelta(days=days_ahead)

    # 设置时间为上午4点，分钟为0（5的倍数），此时美东 0 点
    next_sunday_datetime = datetime.datetime.combine(next_sunday, datetime.time(4, 0))

    # 添加UTC时区信息
    return pytz.UTC.localize(next_sunday_datetime)


def get_next_saturday():
    """获取下一个周六的 datetime 对象，时间设置为上午10点，分钟为5的倍数"""
    now = datetime.datetime.now(pytz.UTC)
    today = now.date()

    # 计算到下一个周六需要的天数 (周六是 weekday() == 5)
    days_ahead = 5 - today.weekday()

    # 如果今天就是周六，我们需要下周的周六
    if days_ahead <= 0:
        days_ahead += 7

    next_saturday = today + datetime.timedelta(days=days_ahead)

    # 设置时间为上午10点，分钟为0（5的倍数）
    next_saturday_datetime = datetime.datetime.combine(
        next_saturday, datetime.time(10, 0)
    )

    # 添加UTC时区信息
    return pytz.UTC.localize(next_saturday_datetime)


async def do_transfer(
    downloader: TikTokDownloader,
    account_uid: str,
    account_mark: str,
    video_id: str,
    video_path: str,
    dest_account_uid: str,
    dest_account_mark: str,
    schedule_time: datetime.datetime = None,
):
    # 上传视频到指定的抖音账户
    downloader.console.print(
        f"开始上传视频 {video_id} 到账户 {dest_account_mark},{dest_account_uid}"
    )
    # 判断视频是否已上传过
    if await downloader.database.has_upload_data(
        dest_account_uid,
        video_id,
    ):
        # 视频已上传过，无需重复上传
        downloader.console.print(
            f"视频 {video_id} 已上传过到账户 {dest_account_mark}，无需重复上传"
        )
        return False

    # todo: 从配置中读取dest_account_uid对应的cookie
    with open("tiktok.com_raw_cookie.json", "r", encoding="utf-8") as f:
        cookies_list = json.load(f)

        basename = os.path.basename(video_path)
        filename, ext = os.path.splitext(basename)
        result = upload_video(
            video_path,
            description=filename.split("-")[-1],
            cookies_list=cookies_list,
            schedule=schedule_time,
        )
        if len(result) == 0:
            # 上传成功
            await downloader.database.write_upload_data(
                dest_account_uid, dest_account_mark, video_id, account_uid, account_mark
            )
            return True
        return False


async def transfer(args):
    async with TikTokDownloader() as downloader:
        try:
            await downloader.init()

            tk_instance = TikTok(
                downloader.parameter,
                downloader.database,
            )

            # 下载更新抖音账户的视频
            await tk_instance.account_detail_batch()

            # 下载更新tiktok账户的视频
            # await tk_instance.account_detail_batch_tiktok()

            # 根据命令行参数确定调度时间
            schedule_time = None
            if args.schedule == 'saturday':
                schedule_time = get_next_saturday()
            elif args.schedule == 'sunday':
                schedule_time = get_next_sunday()

            transfer_configs = downloader.parameter.video_transfer
            for transfer_config in transfer_configs:
                if transfer_config.enable:
                    earliest = transfer_config.earliest
                    recorders = await downloader.recorder.database.read_download_data(
                        mark=transfer_config.from_account_mark,
                        publish_time=(date.today() - timedelta(days=earliest)).strftime(
                            "%Y-%m-%d"
                        ),
                    )
                    # todo: 重复上传过滤、单次上传数量限制
                    num = transfer_config.transfer_num
                    for recorder in recorders:
                        ret = await do_transfer(
                            downloader,
                            transfer_config.from_account_uid,
                            transfer_config.from_account_mark,
                            recorder["ID"],
                            recorder["PATH"],
                            transfer_config.to_account_uid,  # 目标抖音账户UID，暂时实际值为uid_tt
                            transfer_config.to_account_mark,
                            schedule_time,
                        )
                        if ret:
                            num -= 1
                        if num <= 0:
                            break

        except (
            Exception,
            KeyboardInterrupt,
            CancelledError,
        ):
            print("服务异常退出！！！！")
            traceback.print_exc()
            return


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='TikTok视频下载和转移工具')

    # 添加参数
    parser.add_argument(
        '--schedule',
        choices=['none', 'saturday', 'sunday'],
        default='none',
        help='设置上传调度时间: none(立即上传), saturday(下一个周六), sunday(下一个周日)',
    )

    parser.add_argument('--config', type=str, help='指定配置文件路径')
    return parser.parse_args()


if __name__ == "__main__":
    # 解析命令行参数
    args = parse_args()

    run(transfer(args))
