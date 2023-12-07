terraform {
  required_providers {
    twc = {
      source = "tf.timeweb.cloud/timeweb-cloud/timeweb-cloud"
    }
  }
  required_version = ">= 0.13"
}

provider "twc" {
  token = var.token
}

data "twc_configurator" "conf" {
  location = "ru-1"
}
data "twc_software" "docker" {
  name = "Docker"
  os {
    name = "ubuntu"
    version = "22.04"
  }
}

resource "twc_ssh_key" "main" {
  name = "tw_terraform_key"
  body = file("~/.ssh/timeweb.pub")
}

resource "twc_server" "vm1" {
  name = "RG_flow_server"
  os_id = data.twc_software.docker.os[0].id
  software_id = data.twc_software.docker.id

  configuration {
    configurator_id = data.twc_configurator.conf.id
    cpu = 2
    ram = 1024 * 4
    disk = 1024 * 10
  }

  ssh_keys_ids = [twc_ssh_key.main.id]
}