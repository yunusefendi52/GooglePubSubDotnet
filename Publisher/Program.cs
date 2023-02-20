using Google.Api.Gax;
using Google.Cloud.PubSub.V1;

Console.WriteLine("Starting publisher...");

var projectId = "project-app-d1471";
var topicId = "logging-api";

var publisherService = await new PublisherServiceApiClientBuilder
{
    EmulatorDetection = EmulatorDetection.EmulatorOrProduction
}.BuildAsync();

var topicName = new TopicName(projectId, topicId);

if (publisherService.GetTopic(topicName) == null)
{
    await publisherService.CreateTopicAsync(topicName);
}

var publisher = await PublisherClient.CreateAsync(topicName);

Console.WriteLine("Type message");

while (true)
{
    var text = Console.ReadLine();

    await publisher.PublishAsync(text);

    Console.WriteLine($"SENDING: {text}");

    if (text?.Trim().ToLowerInvariant() == "q")
    {
        break;
    }
}

await publisher.ShutdownAsync(TimeSpan.FromSeconds(15));