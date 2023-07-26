import logging
from datetime import datetime

from django.db import models
from django.utils.timezone import make_aware
from django.utils.translation import gettext_lazy as _
from model_utils import Choices, FieldTracker
from model_utils.fields import StatusField
from shortuuid.django_fields import ShortUUIDField

from .model import BaseModelSoftDeletable
from .model_utils import upload_to_without_rename, load_image_from_url

logger = logging.getLogger(__name__)


class AbstractWXMPUser(BaseModelSoftDeletable):
    GENDER_CHOICES = Choices(
        ('male', 'Male'),
        ('female', 'Female'),
        ('unknown', 'Unknown')
    )

    # key
    openid = models.CharField(blank=True, max_length=30, db_index=True)
    unionid = models.CharField(blank=True, max_length=30, default='', db_index=True)
    # attributes
    name = models.CharField(blank=True, max_length=255, default='', db_index=True)
    cellphone = models.CharField(blank=True, max_length=20, default='')
    email = models.CharField(blank=True, max_length=255, default='')
    birthday = models.DateField(blank=True, null=True, default=None)
    gender = StatusField(choices_name='GENDER_CHOICES', default='unknown')
    city = models.CharField(blank=True, max_length=100, default='')
    province = models.CharField(blank=True, max_length=100, default='')
    country = models.CharField(blank=True, max_length=100, default='')
    language = models.CharField(blank=True, max_length=100, default='')
    avatar_url = models.CharField(blank=True, max_length=255, default='', help_text='avatar url')
    avatar = models.ImageField(blank=True, upload_to=upload_to_without_rename, null=True, default=None)
    # time sensitive
    last_login_at = models.DateTimeField(blank=True, null=True, default=None)
    last_info_updated_at = models.DateTimeField(blank=True, null=True, default=None)
    # raw
    raw_data = models.JSONField(blank=True, default=dict)
    # utm tracking
    utm_campaign = models.CharField(blank=True,  max_length=255, default='')
    utm_source = models.CharField(blank=True,  max_length=255, default='')
    # seed
    seed = ShortUUIDField(
        length=7,
        alphabet="23456789abcdefghijkmnopqrstuvwxyz",
        db_index=True
    )

    # the tracker
    tracker = FieldTracker()

    class Meta:
        verbose_name = _('user')
        verbose_name_plural = _('users')
        abstract = True

    @property
    def avatar_link(self):
        return str(self.avatar.url) if self.avatar else ''

    @property
    def is_info_fulfill(self):
        return bool(self.last_info_updated_at)

    @property
    def is_phone_fulfill(self):
        return bool(self.cellphone)

    @classmethod
    def gender_format(cls, value):
        if value == '男' or value == 'male' or value == 1:
            return cls.GENDER_CHOICES.male
        if value == '女' or value == 'female' or value == 2:
            return cls.GENDER_CHOICES.female
        return cls.GENDER_CHOICES.unknown

    @classmethod
    def login(cls, data: dict):
        if 'openid' not in data or not data['openid']:
            raise ValueError('missing openid')

        user, _ = cls.objects.get_or_create(openid=data['openid'])
        if data.get('unionid', ''):
            user.unionid = data.get('unionid', '')

        # utm
        if data.get('utm_source', '') and not data.utm_source:
            user.utm_source = data.get('utm_source', '')
        if data.get('utm_campaign', '') and not data.utm_campaign:
            user.utm_source = data.get('utm_campaign', '')

        user.last_login_at = make_aware(datetime.now())
        user.save()
        return user

    @classmethod
    def update_info(cls, data: dict):
        if 'uid' not in data or not data['uid']:
            raise ValueError('missing uid')

        try:
            user = cls.objects.get(id=data['uid'])
            if data.get('nickName', ''):
                user.name = data.get('nickName', '')
            # if data.get('gender', ''):
            #     user.gender = cls.gender_format(data.get('gender', ''))
            # if data.get('country', ''):
            #     user.country = data.get('country', '')
            # if data.get('province', ''):
            #     user.province = data.get('province', '')
            # if data.get('city', ''):
            #     user.city = data.get('city', '')
            if data.get('language', ''):
                user.language = data.get('language', '')

            if 'avatarUrl' in data and data['avatarUrl'] and data['avatarUrl'] != user.avatar_url:
                user.avatar_url = data['avatarUrl']
                user.avatar = load_image_from_url(user.avatar_url, f'{user.openid}.jpg')

            # user.raw_data = data
            user.last_info_updated_at = make_aware(datetime.now())
            user.save(update_fields=['name', 'language', 'avatar_url', 'avatar', 'last_info_updated_at'])
            return user

        except cls.DoesNotExist:
            logger.error(f'user not exist, data: {data}')
        except cls.MultipleObjectsReturned:
            logger.error(f'multiple user return, data: {data}')

    @classmethod
    def update_phone_v2(cls, data: dict):
        if 'uid' not in data or not data['uid']:
            raise ValueError('missing uid')

        try:
            user = cls.objects.get(id=data['uid'])
            user.cellphone = data['phoneNumber']
            user.save(update_fields=['cellphone'])
        except Exception as err:
            logger.error(str(err))

    @classmethod
    def update_phone(cls, data: dict):
        if 'uid' not in data or not data['uid']:
            raise ValueError('missing uid')

        try:
            user = cls.objects.get(id=data['uid'])
            user.cellphone = data['phone_number']
            user.save()
        except Exception as err:
            logger.error(str(err))

    def __str__(self):
        return '{} - {}'.format(self.name, self.openid)
